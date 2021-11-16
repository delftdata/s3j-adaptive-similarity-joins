package Statistics;

import CustomDataTypes.FinalOutput;
import CustomDataTypes.ShortOutput;
import Statistics.FinalComputations.CombineProcessFunction;
import Statistics.FinalComputations.FinalComputationsReduce;
import Statistics.FinalComputations.FinalComputationsStatsProcess;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.OutputTag;

import java.util.List;

public class LoadBalancingStats {

    public void prepare(SingleOutputStreamOperator<FinalOutput> mainStream,
                        FlinkKafkaProducer<Tuple2<Long, List<Tuple2<Integer, Long>>>> myStatsProducer){

        //<------- comparisons by physical partition per window --------->
        OutputTag<Tuple3<Long, Integer, Long>> lateJoin = new OutputTag<Tuple3<Long, Integer, Long>>("lateJoin"){};
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

}
