import CustomDataTypes.FinalOutput;
import CustomDataTypes.FinalTuple;
import CustomDataTypes.InputTuple;
import CustomDataTypes.SPTuple;
import Operators.*;
import Operators.AdaptivePartitioner.AdapativePartitioner;
import Operators.AdaptivePartitioner.AdaptiveCoPartitioner;
import Operators.AdaptivePartitioner.AdaptivePartitionerCompanion;
import Statistics.AverageLatency;
import Statistics.LatencyMeasure;
import Statistics.OneStepLatencyAverage;
import Utils.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.util.OutputTag;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class onlinePartitioningForSsj {

    private static final Logger LOG = LoggerFactory.getLogger(onlinePartitioningForSsj.class);

    static String pwd = Paths.get("").toAbsolutePath().toString();


    public static void main(String[] args) throws Exception{
        // Arg parsing
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        // Excecution environment details //
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamFactory streamFactory = new StreamFactory(env);
        env.setMaxParallelism(128);
        env.setParallelism(10);
        double dist_threshold = 0.1;

        LOG.info("Enter main.");
        // ========================================================================================================== //

        // OutputTags for sideOutputs. Used to extract information for statistics.
        final OutputTag<Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> sideLCentroids =
                new OutputTag<Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>>("logicalCentroids"){};

        final OutputTag<Tuple3<Long, Integer, Integer>> sideLP =
                new OutputTag<Tuple3<Long, Integer, Integer>>("logicalPartitions"){};

        final OutputTag<Tuple3<Long, Integer, Integer>> sideJoins =
                new OutputTag<Tuple3<Long, Integer, Integer>>("joinComputations"){};

        // ========================================================================================================== //

        // Create the input stream from the source. Set the properties for the centroids.
        int centroidsDim = options.getCentroidsDim();
        int centroidsNum = options.getCentroidsNum();
        HashMap<Integer, Double[]> centroids = SimilarityJoinsUtil.RandomCentroids(centroidsNum, centroidsDim);

        SimilarityJoin similarityOperator;
        SingleOutputStreamOperator<FinalTuple> lpData;

        // The space partitioning operator.
        // Here we partition the incoming data based on proximity of the given centroids and the given threshold.
        // Basically the partitioning is happening by augmenting tuples with key attributes.
        DataStream<InputTuple> firstStream = streamFactory.createDataStream(options.getFirstStream());
        DataStream<SPTuple> ppData1 = firstStream.
                flatMap(new PhysicalPartitioner(dist_threshold, centroids, (env.getMaxParallelism()/env.getParallelism())+1));

        AdaptivePartitionerCompanion adaptivePartitionerCompanion = new AdaptivePartitionerCompanion(dist_threshold, (env.getMaxParallelism()/env.getParallelism())+1);
        adaptivePartitionerCompanion.setSideLPartitions(sideLP);
        adaptivePartitionerCompanion.setSideLCentroids(sideLCentroids);

        KeyedStream<SPTuple, Integer> keyedData = ppData1.keyBy(t -> t.f0);
        if (options.hasSecondStream()) {
            DataStream<InputTuple> secondStream = streamFactory.createDataStream(options.getSecondStream());

            DataStream<SPTuple> ppData2 = secondStream.
                    flatMap(new PhysicalPartitioner(dist_threshold, centroids, (env.getMaxParallelism()/env.getParallelism())+1));

            lpData = keyedData
                    .connect(ppData2.keyBy(t -> t.f0))
                    .process(new AdaptiveCoPartitioner(adaptivePartitionerCompanion));

            similarityOperator = new SimilarityJoin(dist_threshold);
        } else {
            // Write the input stream in a txt file for debugging reasons. Not part of the final pipeline.

            // The group formulation operator.
            // Inside each machine tuples are grouped in threshold-based groups.
            // Again the grouping is happening by augmenting tuples with key attributes.
            lpData = keyedData
                    .process(new AdapativePartitioner(adaptivePartitionerCompanion));

            similarityOperator = new SimilarityJoinSelf(dist_threshold);
        }

        similarityOperator.setSideJoins(sideJoins);
        final OutputTag<Tuple4<Long, Boolean, FinalTuple, FinalTuple>> sideStats = new OutputTag<Tuple4<Long, Boolean, FinalTuple, FinalTuple>>("stats"){};
        firstStream.writeAsText(pwd+"/src/main/resources/streamed_dataset.txt", FileSystem.WriteMode.OVERWRITE);
        SingleOutputStreamOperator<FinalOutput>
                unfilteredSelfJoinedStream = lpData
                .keyBy(new LogicalKeySelector())
                .flatMap(similarityOperator);

        SingleOutputStreamOperator<FinalOutput>
                selfJoinedStream = unfilteredSelfJoinedStream
                .process(new CustomFiltering(sideStats));


        // Measure the average latency per tuple
//        env.setParallelism(1);
//        selfJoinedStream.map(new OneStepLatencyAverage());

        LOG.info(env.getExecutionPlan());

        // Execute
        JobExecutionResult result = env.execute("ssj");
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute");

    }
}
