import CustomDataTypes.*;
import Operators.*;
import Operators.AdaptivePartitioner.AdaptivePartitioner;
import Operators.AdaptivePartitioner.AdaptivePartitionerCompanion;
import Utils.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.OutputTag;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;


public class onlinePartitioningForSsj {

    private static final Logger LOG = LoggerFactory.getLogger(onlinePartitioningForSsj.class);

    static String pwd = Paths.get("").toAbsolutePath().toString();


    public static void main(String[] args) throws Exception{
        // Arg parsing
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        // Execution environment details //
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new HashMapStateBackend());
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


        // Create KafkaProducer for input throughput measurement.
        String throughputTopic = "pipeline-throughput";
        FlinkKafkaProducer<Tuple2<Long, Long>> myThroughputProducer = new FlinkKafkaProducer<>(
                throughputTopic,
                new TypeInformationSerializationSchema<>(TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), env.getConfig()),
                properties
        );

        //Create the broadcasted control stream.
        DataStream<Integer> controlStream = env.addSource(new WindowController(options.getProcessingWindow()));


        MapStateDescriptor<Void, Integer> controlStateDescriptor = new MapStateDescriptor<Void, Integer>(
                "ControlBroadcastState",
                BasicTypeInfo.VOID_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO);

        // broadcast the rules and create the broadcast state
        BroadcastStream<Integer> controlBroadcastStream = controlStream
                .broadcast(controlStateDescriptor);

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
        firstStream.map(t -> new Tuple2<Long, Long>(t.f1, 1L)).returns(TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})).addSink(myThroughputProducer);

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
            secondStream.map(t -> new Tuple2<Long, Long>(t.f1, 1L)).returns(TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})).addSink(myThroughputProducer);

            DataStream<SPTuple> ppData2 = secondStream.
                    flatMap(new PhysicalPartitioner(dist_threshold, centroids, (env.getMaxParallelism()/env.getParallelism())+1)).uid("secondSpacePartitioner");

            lpData = keyedData
                    .connect(ppData2.keyBy(t -> t.f0))
                    .process(new PassthroughCoProcess())
                    .keyBy(t -> t.f0)
                    .connect(controlBroadcastStream)
                    .process(new AdaptivePartitioner(adaptivePartitionerCompanion)).uid("adaptivePartitioner");

            similarityOperator = new SimilarityJoin(dist_threshold);
        } else {
            // Write the input stream in a txt file for debugging reasons. Not part of the final pipeline.

            // The group formulation operator.
            // Inside each machine tuples are grouped in threshold-based groups.
            // Again the grouping is happening by augmenting tuples with key attributes.
            lpData = keyedData
                    .process(new PassthroughProcess())
                    .keyBy(t -> t.f0)
                    .connect(controlBroadcastStream)
                    .process(new AdaptivePartitioner(adaptivePartitionerCompanion)).uid("adaptivePartitioner");

            similarityOperator = new SimilarityJoinSelf(dist_threshold);
        }

        String sideOutputTopic = "pipeline-side-out";
        FlinkKafkaProducer<GroupLevelShortOutput> mySideOutputProducer = new FlinkKafkaProducer<>(
                sideOutputTopic,
                new TypeInformationSerializationSchema<>(TypeInformation.of(new TypeHint<GroupLevelShortOutput>() {}), env.getConfig()),
                properties
        );
        lpData.map(t -> new GroupLevelShortOutput(t.f7, t.f10, t.f2, t.f0, t.f1, Long.valueOf(t.size()))).addSink(mySideOutputProducer);

        similarityOperator.setSideJoins(sideJoins);

        final OutputTag<Tuple4<Long, Boolean, FinalTuple, FinalTuple>> sideStats = new OutputTag<Tuple4<Long, Boolean, FinalTuple, FinalTuple>>("stats"){};
        SingleOutputStreamOperator<FinalOutput>
                unfilteredJoinedStream = lpData
                .keyBy(new LogicalKeySelector())
                .connect(controlBroadcastStream)
                .process(similarityOperator).uid("similarityJoin");

        String outputTopic = "pipeline-out";
        FlinkKafkaProducer<ShortFinalOutput> myOutputProducer = new FlinkKafkaProducer<>(
                outputTopic,
                new TypeInformationSerializationSchema<>(TypeInformation.of(new TypeHint<ShortFinalOutput>() {}), env.getConfig()),
                properties
        );

        unfilteredJoinedStream.map(new ShortFinalOutputMapper()).addSink(myOutputProducer);


        LOG.info(env.getExecutionPlan());

        // Execute
        JobExecutionResult result = env.execute("ssj");

    }
}

