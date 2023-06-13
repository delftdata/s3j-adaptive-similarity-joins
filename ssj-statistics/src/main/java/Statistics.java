import CustomDataTypes.GroupLevelShortOutput;
import CustomDataTypes.ShortFinalOutput;
import CustomDataTypes.ShortOutput;
import StatFunctions.*;
import StatFunctions.FinalComputations.CombineProcessFunction;
import StatFunctions.FinalComputations.FinalComputationsReduce;
import StatFunctions.FinalComputations.FinalComputationsStatsProcess;
import StatFunctions.GroupLevelFinalComputations.GroupLevelCombineProcessFunction;
import StatFunctions.GroupLevelFinalComputations.GroupLevelFinalComputationsReduce;
import StatFunctions.GroupLevelFinalComputations.GroupLevelFinalComputationsStatsProcess;
import StatFunctions.Latency.LatencyMeasure;
import StatFunctions.Latency.Percentiles;
import StatFunctions.Latency.PercentilesCombine;
import StatFunctions.Throughput.InputThroughputProcess;
import StatFunctions.Throughput.InputThroughputReduce;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class Statistics {

    private static final Logger LOG = LoggerFactory.getLogger(Statistics.class);

    static class GroupLevelStatsKeySelector implements KeySelector<GroupLevelShortOutput, Tuple3<Integer, Integer, Integer>> {
        @Override
        public Tuple3<Integer, Integer, Integer> getKey(GroupLevelShortOutput t) throws Exception {
            return new Tuple3<>(t.f3, t.f1, t.f2);
        }
    }

    public static void main(String[] args) throws Exception{
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setMaxParallelism(128);
        env.setParallelism(options.getParallelism());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //Kafka sources
        KafkaSource<ShortFinalOutput> ssjKafkaOutput = KafkaSource.<ShortFinalOutput>builder()
                .setBootstrapServers(options.getKafkaURL())
                .setTopics("pipeline-out")
                .setGroupId("Statistics")
                .setValueOnlyDeserializer(
                        new TypeInformationSerializationSchema<>(TypeInformation.of(
                                new TypeHint<ShortFinalOutput>() { }),
                                env.getConfig())
                )
                .build();

        KafkaSource<GroupLevelShortOutput> ssjKafkaSideOutput = KafkaSource.<GroupLevelShortOutput>builder()
                .setBootstrapServers(options.getKafkaURL())
                .setTopics("pipeline-side-out")
                .setGroupId("Statistics")
                .setValueOnlyDeserializer(
                        new TypeInformationSerializationSchema<>(TypeInformation.of(
                                new TypeHint<GroupLevelShortOutput>() { }),
                                env.getConfig())
                )
                .build();

        KafkaSource<Tuple2<Long, Long>> ssjKafkaThroughput = KafkaSource.<Tuple2<Long, Long>>builder()
                .setBootstrapServers(options.getKafkaURL())
                .setTopics("pipeline-throughput")
                .setGroupId("Statistics")
                .setValueOnlyDeserializer(
                        new TypeInformationSerializationSchema<>(TypeInformation.of(
                                new TypeHint<Tuple2<Long, Long>>() { }),
                                env.getConfig())
                )
                .build();

        WatermarkStrategy<ShortFinalOutput> eventTimeStrategy = WatermarkStrategy.
                <ShortFinalOutput>forBoundedOutOfOrderness(Duration.ofSeconds(2L * options.getWindowLength())).
                withTimestampAssigner((event, timestamp) -> event.f0).withIdleness(Duration.ofSeconds(60));

        WatermarkStrategy<GroupLevelShortOutput> groupEventTimeStrategy = WatermarkStrategy.
                <GroupLevelShortOutput>forBoundedOutOfOrderness(Duration.ofSeconds(2L * options.getWindowLength())).
                withTimestampAssigner((event, timestamp) -> event.f0).withIdleness(Duration.ofSeconds(60));

        WatermarkStrategy<Tuple2<Long, Long>> throughputEventTimeStrategy = WatermarkStrategy.
                <Tuple2<Long, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2L * options.getWindowLength())).
                withTimestampAssigner((event, timestamp) -> event.f0).withIdleness(Duration.ofSeconds(60));

        // Read SSJ outputs.
        DataStream<ShortFinalOutput> ssjOutput = env
                .fromSource(ssjKafkaOutput, eventTimeStrategy, "SSJ-Kafka-Output");

        DataStream<GroupLevelShortOutput> ssjSideOutput = env
                .fromSource(ssjKafkaSideOutput, groupEventTimeStrategy, "SSJ-Kafka-Side-Output");

        DataStream<Tuple2<Long, Long>> ssjThroughput = env
                .fromSource(ssjKafkaThroughput, throughputEventTimeStrategy,"SSJ-Throughput");


        // Kafka producers
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", options.getKafkaURL());
        properties.setProperty("group.id", "percentiles");

        String statsKafkaTopic = "pipeline-out-stats";
        FlinkKafkaProducer<Tuple2<Long, List<Tuple2<Integer, List<Tuple2<String, Long>>>>>> averageLatencyPercentiles =
                new FlinkKafkaProducer<Tuple2<Long, List<Tuple2<Integer, List<Tuple2<String, Long>>>>>>(
                        statsKafkaTopic,
                        new ObjectSerializationSchema<Tuple2<Long, List<Tuple2<Integer, List<Tuple2<String, Long>>>>>>(
                                options.getJobID() + "_latency-percentiles-per-machine", statsKafkaTopic),
                        properties,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                );

        FlinkKafkaProducer<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>> groupLevelFinalComputationsProducer =
                new FlinkKafkaProducer<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>>(
                        statsKafkaTopic,
                        new ObjectSerializationSchema<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>>(options.getJobID() + "_final-comps-per-group", statsKafkaTopic),
                        properties,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                );

        FlinkKafkaProducer<Tuple2<Long, List<Tuple2<Integer, Long>>>> machineLevelFinalComputationsProducer =
                new FlinkKafkaProducer<Tuple2<Long, List<Tuple2<Integer, Long>>>>(
                        statsKafkaTopic,
                        new ObjectSerializationSchema<Tuple2<Long, List<Tuple2<Integer, Long>>>>(options.getJobID() + "_final-comps-per-machine", statsKafkaTopic),
                        properties,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                );

        FlinkKafkaProducer<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>> groupSizeProducer =
                new FlinkKafkaProducer<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>>(
                        statsKafkaTopic,
                        new ObjectSerializationSchema<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>>(options.getJobID() + "_size-per-group", statsKafkaTopic),
                        properties,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                );

        FlinkKafkaProducer<Tuple2<Long, Long>> inputThroughputProducer =
                new FlinkKafkaProducer<Tuple2<Long, Long>>(
                        statsKafkaTopic,
                        new ObjectSerializationSchema<Tuple2<Long, Long>>(options.getJobID() + "_throughput", statsKafkaTopic),
                        properties,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                );


        // Compute Latency Percentiles per window and machine.
        ssjOutput
                .map(t -> new Tuple3<Integer, Long, Long>(t.f4, t.f1, t.f0))
                .returns(TypeInformation.of(new TypeHint<Tuple3<Integer, Long, Long>>() {}))
                .keyBy(t -> t.f0)
                .process(new LatencyMeasure())
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(options.getWindowLength())))
                .process(new Percentiles())
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(options.getWindowLength())))
                .process(new PercentilesCombine())
                .addSink(averageLatencyPercentiles);


        // Compute stats for final comparisons per window and machine.
        ssjOutput
                .map(t -> {
                    if(t.f7){
                        return new ShortOutput(t.f0, t.f4, 1L);
                    }
                    else{
                        return new ShortOutput(t.f0, t.f4, 10_000L);
                    }
                })
                .returns(TypeInformation.of(ShortOutput.class))
                .keyBy(t -> t.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(options.getWindowLength())))
                .reduce(new FinalComputationsReduce(), new FinalComputationsStatsProcess())
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(options.getWindowLength())))
                .process(new CombineProcessFunction())
                .addSink(machineLevelFinalComputationsProducer);


        // Compute stats for final comparisons per window and group
//        ssjOutput
//                .map(t -> new GroupLevelShortOutput(t.f0, t.f4, t.f5, t.f6, 1L))
//                .returns(TypeInformation.of(GroupLevelShortOutput.class))
//                .keyBy(new GroupLevelStatsKeySelector())
//                .window(TumblingEventTimeWindows.of(Time.seconds(options.getWindowLength())))
//                .reduce(new GroupLevelFinalComputationsReduce(), new GroupLevelFinalComputationsStatsProcess())
//                .windowAll(TumblingEventTimeWindows.of(Time.seconds(options.getWindowLength())))
//                .process(new GroupLevelCombineProcessFunction())
//                .addSink(groupLevelFinalComputationsProducer);

//        env.setParallelism(1);
        ssjSideOutput
                .map(t -> new GroupLevelShortOutput(t.f0, t.f1, t.f2, t.f3, t.f4, 1L))
                .returns(TypeInformation.of(GroupLevelShortOutput.class))
                .keyBy(new RecordTypeLogicalPartitionSelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(options.getWindowLength())))
                .sum(5)
                .map(new RecordTypeLPMapper())
                .keyBy(new GroupLevelKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(options.getWindowLength())))
                .process(new LLCostCalculator())
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(options.getWindowLength())))
                .process(new GroupLevelCombineLoadEstimations())
                .addSink(groupLevelFinalComputationsProducer);


        // Compute group sizes per window.
        ssjSideOutput
                .keyBy(new GroupLevelStatsKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(options.getWindowLength())))
                .reduce(new GroupLevelFinalComputationsReduce(), new GroupLevelFinalComputationsStatsProcess())
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(options.getWindowLength())))
                .process(new GroupLevelCombineProcessFunction())
                .addSink(groupSizeProducer);

        ssjThroughput
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(options.getWindowLength())))
                .reduce(new InputThroughputReduce(), new InputThroughputProcess())
                .addSink(inputThroughputProducer);

//        env.setParallelism(options.getParallelism());


        LOG.info(env.getExecutionPlan());
        env.execute("stats");

    }
}
