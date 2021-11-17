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
import Utils.ObjectSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.List;
import java.util.Properties;

public class LoadBalancingStats {

    private final Properties properties;
    private final String statsKafkaTopic;

    public LoadBalancingStats(Properties properties, String statsKafkaTopic){
        this.properties = properties;
        this.statsKafkaTopic = statsKafkaTopic;
    }

    class GroupLevelStatsKeySelector implements KeySelector<GroupLevelShortOutput, Tuple3<Integer,Integer, Integer>>{
        @Override
        public Tuple3<Integer, Integer, Integer> getKey(GroupLevelShortOutput t) throws Exception {
            return new Tuple3<>(t.f3, t.f1, t.f2);
        }
    }

    public void prepareFinalComputationsPerMachine(SingleOutputStreamOperator<FinalOutput> mainStream){

        FlinkKafkaProducer<Tuple2<Long, List<Tuple2<Integer, Long>>>> myStatsProducer =
                new FlinkKafkaProducer<Tuple2<Long, List<Tuple2<Integer, Long>>>>(
                        statsKafkaTopic,
                        new ObjectSerializationSchema<Tuple2<Long, List<Tuple2<Integer, Long>>>>("final-comps-per-machine", statsKafkaTopic),
                        properties,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                );

        //<------- comparisons by physical partition per window --------->
        SingleOutputStreamOperator<Tuple2<Long, List<Tuple2<Integer, Long>>>> check =
        mainStream
                .map(t -> new ShortOutput(t.f3, t.f1.f10, 1L))
                .returns(TypeInformation.of(ShortOutput.class))
                .keyBy(t -> t.f1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new FinalComputationsReduce(),new FinalComputationsStatsProcess())
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new CombineProcessFunction());

        check.addSink(myStatsProducer);
    }

    public void prepareFinalComputationsPerGroup(SingleOutputStreamOperator<FinalOutput> mainStream){

        FlinkKafkaProducer<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>> groupLevelFinalComputationsProducer =
                new FlinkKafkaProducer<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>>(
                        statsKafkaTopic,
                        new ObjectSerializationSchema<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>>("final-comps-per-group", statsKafkaTopic),
                        properties,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                );

        SingleOutputStreamOperator<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>> check =
                mainStream
                        .map(t -> new GroupLevelShortOutput(t.f3, t.f1.f10, t.f1.f2, t.f1.f0, 1L))
                        .returns(TypeInformation.of(GroupLevelShortOutput.class))
                        .keyBy(new GroupLevelStatsKeySelector())
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .reduce(new GroupLevelFinalComputationsReduce(),new GroupLevelFinalComputationsStatsProcess())
                        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .process(new GroupLevelCombineProcessFunction());

        check.addSink(groupLevelFinalComputationsProducer);
    }

    public void prepareSizePerGroup(SingleOutputStreamOperator<FinalTuple> mainStream){

        FlinkKafkaProducer<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>> groupSizeProducer =
                new FlinkKafkaProducer<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>>(
                        statsKafkaTopic,
                        new ObjectSerializationSchema<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>>("size-per-group", statsKafkaTopic),
                        properties,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                );

        SingleOutputStreamOperator<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>> check =
                mainStream
                        .map(t -> new GroupLevelShortOutput(t.f7, t.f10, t.f2, t.f0, Long.valueOf(t.size())))
                        .returns(TypeInformation.of(GroupLevelShortOutput.class))
                        .keyBy(new GroupLevelStatsKeySelector())
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .reduce(new GroupLevelFinalComputationsReduce(),new GroupLevelFinalComputationsStatsProcess())
                        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .process(new GroupLevelCombineProcessFunction());

        check.addSink(groupSizeProducer);
    }

}
