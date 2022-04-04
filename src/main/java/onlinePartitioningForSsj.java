import CustomDataTypes.*;
import Operators.AdaptivePartitioner.AdaptiveCoPartitioner;
import Operators.AdaptivePartitioner.AdaptivePartitioner;
import Operators.AdaptivePartitioner.AdaptivePartitionerCompanion;
import Operators.PhysicalPartitioner;
import Operators.SimilarityJoin;
import Operators.SimilarityJoinSelf;
import Statistics.LoadBalancingStats;
import Utils.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.OutputTag;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;


public class onlinePartitioningForSsj {

    private static final Logger LOG = LoggerFactory.getLogger(onlinePartitioningForSsj.class);

    static String pwd = Paths.get("").toAbsolutePath().toString();

//    static String jobUUID = UUID.randomUUID().toString();

    public static void main(String[] args) throws Exception{
        // Arg parsing
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        // Execution environment details //
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new RocksDBStateBackend("s3://flink/checkpoints/", true));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", options.getKafkaURL());
        Properties leftInputProperties = new Properties();
        leftInputProperties.setProperty("bootstrap.servers", options.getKafkaURL());
        leftInputProperties.setProperty("group.id", "LeftStream");
        Properties rightInputProperties = new Properties();
        rightInputProperties.setProperty("bootstrap.servers", options.getKafkaURL());
        rightInputProperties.setProperty("group.id", "RightStream");


        String leftInputTopic = "pipeline-in-left";
        String rightInputTopic = "pipeline-in-right";
        env.setMaxParallelism(128);
        env.setParallelism(options.getParallelism());
        double dist_threshold = 1.0 - options.getThreshold();

        LOG.info("Enter main.");
        // ========================================================================================================== //


        // OutputTags for sideOutputs. Used to extract information for statistics.
        final OutputTag<Tuple2<Integer, HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> sideLCentroids =
                new OutputTag<Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>>("logicalCentroids"){};

        final OutputTag<Tuple3<Long, Integer, Integer>> sideLP =
                new OutputTag<Tuple3<Long, Integer, Integer>>("logicalPartitions"){};

        final OutputTag<Tuple3<Long, Integer, Integer>> sideJoins =
                new OutputTag<Tuple3<Long, Integer, Integer>>("joinComputations"){};

        // ========================================================================================================== //

        // Create the input stream from the source. Set the properties for the centroids.
        int centroidsDim = options.getCentroidsDim();
        int centroidsNum = options.getParallelism();
        HashMap<Integer, Double[]> centroids = SimilarityJoinsUtil.RandomCentroids(centroidsNum, centroidsDim);

        SimilarityJoin similarityOperator;
        SingleOutputStreamOperator<FinalTuple> lpData;

        // The space partitioning operator.
        // Here we partition the incoming data based on proximity of the given centroids and the given threshold.
        // Basically the partitioning is happening by augmenting tuples with key attributes.
        KafkaSource<InputTuple> leftKafkaStream = KafkaSource.<InputTuple>builder()
                .setProperties(leftInputProperties)
                .setTopics(leftInputTopic)
                .setValueOnlyDeserializer(new TypeInformationSerializationSchema<>(TypeInformation.of(new TypeHint<InputTuple>() { }), env.getConfig()))
                .build();
        DataStream<InputTuple> firstStream = env
                .fromSource(leftKafkaStream, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Left Stream")
                .map(new IngestTimeMapper());
        DataStream<SPTuple> ppData1 = firstStream.
                flatMap(new PhysicalPartitioner(dist_threshold, centroids, (env.getMaxParallelism()/env.getParallelism())+1)).uid("firstSpacePartitioner");

        AdaptivePartitionerCompanion adaptivePartitionerCompanion = new AdaptivePartitionerCompanion(dist_threshold, (env.getMaxParallelism()/env.getParallelism())+1);
        adaptivePartitionerCompanion.setSideLPartitions(sideLP);
        adaptivePartitionerCompanion.setSideLCentroids(sideLCentroids);

        KeyedStream<SPTuple, Integer> keyedData = ppData1.keyBy(t -> t.f0);
        if (options.hasSecondStream()) {
            KafkaSource<InputTuple> rightKafkaStream = KafkaSource.<InputTuple>builder()
                    .setProperties(rightInputProperties)
                    .setTopics(rightInputTopic)
                    .setValueOnlyDeserializer(new TypeInformationSerializationSchema<>(TypeInformation.of(new TypeHint<InputTuple>() { }), env.getConfig()))
                    .build();
            DataStream<InputTuple> secondStream = env
                    .fromSource(rightKafkaStream, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Right Stream")
                    .map(new IngestTimeMapper());

            DataStream<SPTuple> ppData2 = secondStream.
                    flatMap(new PhysicalPartitioner(dist_threshold, centroids, (env.getMaxParallelism()/env.getParallelism())+1)).uid("secondSpacePartitioner");

            lpData = keyedData
                    .connect(ppData2.keyBy(t -> t.f0))
                    .process(new AdaptiveCoPartitioner(adaptivePartitionerCompanion)).uid("adaptivePartitioner");

            similarityOperator = new SimilarityJoin(dist_threshold);
        } else {
            // Write the input stream in a txt file for debugging reasons. Not part of the final pipeline.

            // The group formulation operator.
            // Inside each machine tuples are grouped in threshold-based groups.
            // Again the grouping is happening by augmenting tuples with key attributes.
            lpData = keyedData
                    .process(new AdaptivePartitioner(adaptivePartitionerCompanion)).uid("adaptivePartitioner");

            similarityOperator = new SimilarityJoinSelf(dist_threshold);
        }

        similarityOperator.setSideJoins(sideJoins);
        final OutputTag<Tuple4<Long, Boolean, FinalTuple, FinalTuple>> sideStats = new OutputTag<Tuple4<Long, Boolean, FinalTuple, FinalTuple>>("stats"){};
        SingleOutputStreamOperator<FinalOutput>
                unfilteredJoinedStream = lpData
                .keyBy(new LogicalKeySelector())
                .flatMap(similarityOperator).uid("similarityJoin");


        String outputTopic = "pipeline-out";
        FlinkKafkaProducer<ShortFinalOutput> myProducer = new FlinkKafkaProducer<>(
                outputTopic,
                new TypeInformationSerializationSchema<>(TypeInformation.of(new TypeHint<ShortFinalOutput>() {}), env.getConfig()),
                properties
        );

        String outputStatsTopic = "pipeline-out-stats";
        String allLatenciesTopic = "all-latencies";
//        LoadBalancingStats stats = new LoadBalancingStats(jobUUID, properties, outputStatsTopic, allLatenciesTopic,options.getWindowLength());
//        stats.prepareFinalComputationsPerMachine(unfilteredSelfJoinedStream);
//        stats.prepareFinalComputationsPerGroup(unfilteredSelfJoinedStream);
//        stats.prepareSizePerGroup(lpData);
//        stats.prepareLatencyPerMachine(unfilteredSelfJoinedStream);
//        stats.prepareSampledLatencyPercentilesPerMachine(unfilteredJoinedStream, 0.2);

//        SingleOutputStreamOperator<FinalOutput>
//                selfJoinedStream = unfilteredJoinedStream
//                .process(new CustomFiltering(sideStats));

        unfilteredJoinedStream.map(new ShortFinalOutputMapper()).addSink(myProducer);

//        stream.addSink(myProducer);
        // Measure the average latency per tuple
//        env.setParallelism(1);
//        selfJoinedStream.map(new OneStepLatencyAverage());

        LOG.info(env.getExecutionPlan());

        // Execute
        JobExecutionResult result = env.execute("ssj");
//        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute");

    }
}


// TODO: Add a choice of distance functions.
