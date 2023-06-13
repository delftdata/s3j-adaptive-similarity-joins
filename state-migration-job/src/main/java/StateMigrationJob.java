import BootstrapFunctions.GroupFormulationBootstrapFunction;
import BootstrapFunctions.SimilarityJoinBootstrapFunction;
import CustomDataTypes.GroupFormulationState;
import CustomDataTypes.SimilarityJoinState;
import Deserializers.LogicalKeyDeserializer;
import ModificationFunctions.ReKeySimilarityJoinState;
import ModificationFunctions.UpdateAllocationMap;
import StateReaderFunctions.GroupFormulationStateReader;
import StateReaderFunctions.SimilarityJoinStateReader;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.*;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

// Note: For when checkpointing/savepointing a job an iteration inside an operator can cause some duplicate results.
// When the job will restart, it will start again the iteration instead of continuing from where it stopped.

public class StateMigrationJob {

    private static final Logger LOG = LoggerFactory.getLogger(StateMigrationJob.class);

    public static void main(String[] args) throws Exception{

        // Parsing arguments
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        options.setArgs();

        // Minio configuration
        MinioClient minioClient =
                MinioClient.builder()
                        .endpoint(options.getMinioLocation())
                        .credentials(options.getMinioAccessKey(), options.getMinioSecretKey())
                        .build();

        // Create Gson with the appropriate deserializer
        GsonBuilder gsonBuilder= new GsonBuilder();
        gsonBuilder.registerTypeAdapter(new TypeToken<Tuple3<Integer, Integer, Integer>>(){}.getType(), new LogicalKeyDeserializer());
        Gson gson = gsonBuilder.create();

        // Read input file from Minio
        InputStream groupFile = minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(options.getGroupAllocBucket())
                        .object(options.getGroupAllocFilename())
                        .build());
        String data = IOUtils.toString(groupFile, StandardCharsets.UTF_8);
        HashMap<Tuple3<Integer, Integer, Integer>, Integer> newAllocations = gson.fromJson(data, new TypeToken<HashMap<Tuple3<Integer,Integer,Integer>, Integer>>(){}.getType());
        groupFile.close();

        LOG.info("Allocation file read successfully.");
        // Read savepoint
        ExecutionEnvironment bEnv   = ExecutionEnvironment.getExecutionEnvironment();
        bEnv.setParallelism(1);
        ExistingSavepoint savepoint = Savepoint.load(bEnv, options.getSavepointName(), new HashMapStateBackend());

        LOG.info("Savepoint loaded successfully.");

        // Read keyed state from savepoint
        DataSet<GroupFormulationState> groupFormulationState = savepoint.readKeyedState("adaptivePartitioner", new GroupFormulationStateReader());
        DataSet<SimilarityJoinState> similarityJoinState = savepoint.readKeyedState("similarityJoin", new SimilarityJoinStateReader());

        LOG.info("Keyed states read successfully.");

        //Modify groupFormulationState based on the group allocation file.
        DataSet<GroupFormulationState> updatedGroupFormulationState =
                groupFormulationState.map(new UpdateAllocationMap(newAllocations));

        // Modify similarityJoinState based on the group allocation file.
        DataSet<SimilarityJoinState> updatedSimilarityJoinState =
                similarityJoinState.map(new ReKeySimilarityJoinState(newAllocations));

        LOG.info("Keyed states modified successfully.");

        // Transformations for rewriting the state
        BootstrapTransformation<GroupFormulationState> groupFormulationTransformation = OperatorTransformation
                .bootstrapWith(updatedGroupFormulationState)
                .keyBy(x -> x.key)
                .transform(new GroupFormulationBootstrapFunction());

        BootstrapTransformation<SimilarityJoinState> similarityJoinTransformation = OperatorTransformation
                .bootstrapWith(updatedSimilarityJoinState)
                .keyBy(new KeySelector<SimilarityJoinState, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> getKey(SimilarityJoinState state) throws Exception {
                        return state.key;
                    }
                })
                .transform(new SimilarityJoinBootstrapFunction());

        // Create a new savepoint
        Savepoint
                .create(new HashMapStateBackend(), 128)
                .withOperator("adaptivePartitioner", groupFormulationTransformation)
                // .withOperator("similarityJoin", similarityJoinTransformation)
                .write(options.getSavepointName()+"_updated");

        LOG.info("New savepoint created successfully.");

        bEnv.execute();

    }

}
