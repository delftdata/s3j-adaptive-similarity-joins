package Statistics;

import CustomDataTypes.FinalOutput;
import CustomDataTypes.FinalTuple;
import CustomDataTypes.GroupLevelShortOutput;
import CustomDataTypes.ShortOutput;
import Statistics.FinalComputations.CombineProcessFunction;
import Statistics.FinalComputations.FinalComputationsReduce;
import Statistics.FinalComputations.FinalComputationsStatsProcess;
import Statistics.GroupLevelFinalComputations.GroupLevelCombineProcessFunction;
import Statistics.GroupLevelFinalComputations.GroupLevelFinalComputationsReduce;
import Statistics.GroupLevelFinalComputations.GroupLevelFinalComputationsStatsProcess;
import Statistics.Latency.*;
import Utils.ObjectSerializationSchema;
import Utils.ShortFinalOutputMapper;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Properties;

public class LoadBalancingStats {

    private final Properties properties;
    private final String statsKafkaTopic;
    private final int windowLength;
    private final String allLatenciesTopic;
    private final String jobUUID;

    public LoadBalancingStats(String jobUUID, Properties properties, String statsKafkaTopic, String allLatenciesTopic,
                              int windowLength) {
        this.jobUUID = jobUUID;
        this.properties = properties;
        this.statsKafkaTopic = statsKafkaTopic;
        this.windowLength = windowLength;
        this.allLatenciesTopic = allLatenciesTopic;
    }

    class GroupLevelStatsKeySelector implements KeySelector<GroupLevelShortOutput, Tuple3<Integer, Integer, Integer>> {
        @Override
        public Tuple3<Integer, Integer, Integer> getKey(GroupLevelShortOutput t) throws Exception {
            return new Tuple3<>(t.f3, t.f1, t.f2);
        }
    }

    public void prepareFinalComputationsPerMachine(SingleOutputStreamOperator<FinalOutput> mainStream) {

        FlinkKafkaProducer<Tuple2<Long, List<Tuple2<Integer, Long>>>> myStatsProducer =
                new FlinkKafkaProducer<Tuple2<Long, List<Tuple2<Integer, Long>>>>(
                        statsKafkaTopic,
                        new ObjectSerializationSchema<Tuple2<Long, List<Tuple2<Integer, Long>>>>(jobUUID+"_final-comps-per-machine", statsKafkaTopic),
                        properties,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                );

        //<------- comparisons by physical partition per window --------->
        OutputTag<ShortOutput> lateMachine = new OutputTag<ShortOutput>("lateMachine") {
        };
        SingleOutputStreamOperator<Tuple2<Long, List<Tuple2<Integer, Long>>>> check =
                mainStream
                        .map(t -> new ShortOutput(t.f3, t.f1.f10, 1L))
                        .returns(TypeInformation.of(ShortOutput.class))
                        .keyBy(t -> t.f1)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(windowLength)))
                        .sideOutputLateData(lateMachine)
                        .reduce(new FinalComputationsReduce(), new FinalComputationsStatsProcess())
                        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(windowLength)))
                        .process(new CombineProcessFunction());

        check.addSink(myStatsProducer);
    }

//    public void prepareFinalComputationsPerGroup(SingleOutputStreamOperator<FinalOutput> mainStream) {
//
//        FlinkKafkaProducer<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>> groupLevelFinalComputationsProducer =
//                new FlinkKafkaProducer<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>>(
//                        statsKafkaTopic,
//                        new ObjectSerializationSchema<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>>(jobUUID+"_final-comps-per-group", statsKafkaTopic),
//                        properties,
//                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
//                );
//
//        SingleOutputStreamOperator<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>> check =
//                mainStream
//                        .map(t -> new GroupLevelShortOutput(t.f3, t.f1.f10, t.f1.f2, t.f1.f0,   1L))
//                        .returns(TypeInformation.of(GroupLevelShortOutput.class))
//                        .keyBy(new GroupLevelStatsKeySelector())
//                        .window(TumblingProcessingTimeWindows.of(Time.seconds(windowLength)))
//                        .reduce(new GroupLevelFinalComputationsReduce(), new GroupLevelFinalComputationsStatsProcess())
//                        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(windowLength)))
//                        .process(new GroupLevelCombineProcessFunction());
//
//        check.addSink(groupLevelFinalComputationsProducer);
//    }

//    public void prepareSizePerGroup(SingleOutputStreamOperator<FinalTuple> mainStream) {
//
//        FlinkKafkaProducer<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>> groupSizeProducer =
//                new FlinkKafkaProducer<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>>(
//                        statsKafkaTopic,
//                        new ObjectSerializationSchema<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>>(jobUUID+"_size-per-group", statsKafkaTopic),
//                        properties,
//                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
//                );
//
//        SingleOutputStreamOperator<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>> check =
//                mainStream
//                        .map(t -> new GroupLevelShortOutput(t.f7, t.f10, t.f2, t.f0, Long.valueOf(t.size())))
//                        .returns(TypeInformation.of(GroupLevelShortOutput.class))
//                        .keyBy(new GroupLevelStatsKeySelector())
//                        .window(TumblingProcessingTimeWindows.of(Time.seconds(windowLength)))
//                        .reduce(new GroupLevelFinalComputationsReduce(), new GroupLevelFinalComputationsStatsProcess())
//                        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(windowLength)))
//                        .process(new GroupLevelCombineProcessFunction());
//
//        check.addSink(groupSizeProducer);
//    }

//    public void prepareLatencyPerMachine(SingleOutputStreamOperator<FinalOutput> mainStream) {
//
//        FlinkKafkaProducer<Tuple2<Long, List<Tuple3<Integer, Long, Long>>>> averageLatencyPerMachine =
//                new FlinkKafkaProducer<Tuple2<Long, List<Tuple3<Integer, Long, Long>>>>(
//                        statsKafkaTopic,
//                        new ObjectSerializationSchema<Tuple2<Long, List<Tuple3<Integer, Long, Long>>>>(jobUUID+"_av-latency-per-machine", statsKafkaTopic),
//                        properties,
//                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
//                );
//
//        SingleOutputStreamOperator<Tuple2<Long, List<Tuple3<Integer, Long, Long>>>> check =
//                mainStream
//                        .map(new ShortFinalOutputMapper())
//                        .keyBy(t -> t.f4)
//                        .window(TumblingProcessingTimeWindows.of(Time.seconds(windowLength)))
//                        .aggregate(new LatencyAggregate(), new LatencyProcess())
//                        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(windowLength)))
//                        .process(new LatencyCombine());
//
//        check.addSink(averageLatencyPerMachine);
//
//    }

    public void writeLatenciesToKafka(SingleOutputStreamOperator<FinalOutput> mainStream) {

        FlinkKafkaProducer<Tuple2<Long, Tuple2<Integer, Long>>> kafkaLatencies =
                new FlinkKafkaProducer<Tuple2<Long, Tuple2<Integer, Long>>>(
                        statsKafkaTopic,
                        new ObjectSerializationSchema<Tuple2<Long, Tuple2<Integer, Long>>>(jobUUID+"_latencies-per-window", allLatenciesTopic),
                        properties,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                );

        SingleOutputStreamOperator<Tuple2<Long, Tuple2<Integer, Long>>> check =
                mainStream
                        .map(t -> new Tuple4<Long, Integer, Long, Long>(t.f3, t.f1.f2, t.f1.f7, t.f2.f7))
                        .returns(TypeInformation.of(new TypeHint<Tuple4<Long, Integer, Long, Long>>() {}))
                        .keyBy(t -> t.f1)
                        .process(new LatencyMeasure())
                        .keyBy(t -> t.f0)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(windowLength)))
                        .process(new AddWindowStartTime());

        check.addSink(kafkaLatencies);

    }

    public void prepareSampledLatencyPercentilesPerMachine(SingleOutputStreamOperator<FinalOutput> mainStream,
                                                           double samplingProbability){

        FlinkKafkaProducer<Tuple2<Long, List<Tuple2<Integer, List<Tuple2<String, Long>>>>>> averageLatencyPercentiles =
                new FlinkKafkaProducer<Tuple2<Long, List<Tuple2<Integer, List<Tuple2<String, Long>>>>>>(
                        statsKafkaTopic,
                        new ObjectSerializationSchema<Tuple2<Long, List<Tuple2<Integer, List<Tuple2<String, Long>>>>>>(jobUUID+"_latency-percentiles-per-machine", statsKafkaTopic),
                        properties,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                );

        SingleOutputStreamOperator<Tuple2<Long, List<Tuple2<Integer, List<Tuple2<String, Long>>>>>> check =
                mainStream
                        .process(new BernoulliSampling(samplingProbability))
                        .map(t -> new Tuple4<Long, Integer, Long, Long>(t.f3, t.f1.f10, t.f1.f7, t.f2.f7))
                        .returns(TypeInformation.of(new TypeHint<Tuple4<Long, Integer, Long, Long>>() {}))
                        .keyBy(t -> t.f1)
                        .process(new LatencyMeasure())
                        .keyBy(t -> t.f0)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(windowLength)))
                        .process(new Percentiles())
                        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(windowLength)))
                        .process(new PercentilesCombine());

        check.addSink(averageLatencyPercentiles);

    }

}
