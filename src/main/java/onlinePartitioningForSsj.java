import CustomDataTypes.FinalOutput;
import CustomDataTypes.FinalTuple;
import CustomDataTypes.SPTuple;
import Operators.AdaptivePartitioner;
import Operators.PhysicalPartitioner;
import Operators.SimilarityJoin;
import Statistics.AverageLatency;
import Statistics.LatencyMeasure;
import Statistics.OneStepLatencyAverage;
import Utils.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
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

        // Set parallelism to 1 while ingesting the source.
        env.setParallelism(1);

        // ========================================================================================================== //

        // Create the input stream from the source. Set the properties for the centroids.
        DataStream<Tuple4<Long, Long, Integer, Double[]>> firstStream = streamFactory.createDataStream(options.getFirstStream());
        int centroidsDim = options.getCentroidsDim();
        int centroidsNum = options.getCentroidsNum();

        if (options.hasSecondStream()) {
            DataStream<Tuple4<Long, Long, Integer, Double[]>> secondStream = streamFactory.createDataStream(options.getSecondStream());
            // Handle 2 stream case here
            HashMap<Integer, Double[]> centroids = SimilarityJoinsUtil.RandomCentroids(centroidsNum, centroidsDim);

            DataStream<SPTuple> ppData1 = firstStream.
                    flatMap(new PhysicalPartitioner(0.1, centroids, (env.getMaxParallelism()/env.getParallelism())+1));

            DataStream<SPTuple> ppData2 = secondStream.
                    flatMap(new PhysicalPartitioner(0.1, centroids, (env.getMaxParallelism()/env.getParallelism())+1));

            SingleOutputStreamOperator<FinalTuple> lpData = ppData1
                    .keyBy(t -> t.f0)
                    .connect(ppData2.keyBy(t -> t.f0))
                    .process(new AdaptivePartitioner(0.1, (env.getMaxParallelism()/env.getParallelism())+1, LOG, sideLP, sideLCentroids));

            final OutputTag<Tuple4<Long, Boolean, FinalTuple, FinalTuple>> sideStats =
                    new OutputTag<Tuple4<Long, Boolean, FinalTuple, FinalTuple>>("stats"){};

            SingleOutputStreamOperator<FinalOutput>
                    unfilteredSelfJoinedStream = lpData
                    .keyBy(new LogicalKeySelector())
                    .flatMap(new SimilarityJoin(0.1, LOG, sideJoins, selfJoin));

            SingleOutputStreamOperator<FinalOutput>
                    selfJoinedStream = unfilteredSelfJoinedStream
                    .process(new CustomFiltering(sideStats));
        } else {
            // Handle 1 stream case here
        }



        // ========================================================================================================== //

        // Write the input stream in a txt file for debugging reasons. Not part of the final pipeline.
        firstStream.writeAsText(pwd+"/src/main/resources/streamed_dataset.txt", FileSystem.WriteMode.OVERWRITE);

        // Main pipeline - Only for self joins. //

        env.setParallelism(10);

        // The space partitioning operator.
        // Here we partition the incoming data based on proximity of the given centroids and the given threshold.
        // Basically the partitioning is happening by augmenting tuples with key attributes.
        DataStream<SPTuple> ppData = data.
                flatMap(new PhysicalPartitioner(0.1, SimilarityJoinsUtil.RandomCentroids(centroidsNum, centroidsDim), (env.getMaxParallelism()/env.getParallelism())+1));

        // The group formulation operator.
        // Inside each machine tuples are grouped in threshold-based groups.
        // Again the grouping is happening by augmenting tuples with key attributes.
        SingleOutputStreamOperator<FinalTuple> lpData = ppData
                .keyBy(t -> t.f0)
                .process(new AdaptivePartitioner(0.1, (env.getMaxParallelism()/env.getParallelism())+1, LOG, sideLP, sideLCentroids));

        // Another statistics related side output.
        final OutputTag<Tuple4<Long, Boolean, FinalTuple, FinalTuple>> sideStats =
                new OutputTag<Tuple4<Long, Boolean, FinalTuple, FinalTuple>>("stats"){};


        // The similarity computation operator. - TO BE UPDATED.
        // Global windowing is used for full history joins. A custom trigger is applied for on element processing.
        SingleOutputStreamOperator<FinalOutput>
                unfilteredSelfJoinedStream = lpData
                .keyBy(new LogicalKeySelector())
                .flatMap(new SimilarityJoin(0.1, LOG, sideJoins));


        // Filter the pairs that match.
        SingleOutputStreamOperator<FinalOutput>
                selfJoinedStream = unfilteredSelfJoinedStream
                .process(new CustomFiltering(sideStats));

        // ========================================================================================================== //

        // Measure the average latency per tuple
//        env.setParallelism(1);
//        selfJoinedStream.map(new OneStepLatencyAverage());

        LOG.info(env.getExecutionPlan());

        // Execute
        JobExecutionResult result = env.execute("ssj");
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute");

    }
}
