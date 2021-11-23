package Statistics;

import CustomDataTypes.FinalOutput;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class LoadBalancingStats {

    public void prepare(SingleOutputStreamOperator<FinalOutput> mainStream,
                        OutputTag<Tuple3<Long, Integer, Integer>> sideJoins,
                        String pwd){

        //<------- comparisons by physical partition per window --------->
        OutputTag<Tuple3<Long, Integer, Long>> lateJoin = new OutputTag<Tuple3<Long, Integer, Long>>("lateJoin"){};
        SingleOutputStreamOperator<Tuple3<Long,Integer,Long>> check =
        mainStream
                .map(t -> new Tuple3<Long, Integer, Long>(t.f3, t.f1.f10, 1L))
                .returns(TypeInformation.of(new TypeHint<Tuple3<Long, Integer, Long>>() {}))
                .keyBy(t -> t.f1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .sideOutputLateData(lateJoin)
                .sum(2);

        check.getSideOutput(lateJoin).print();

        check
                .map(new StatsMappers.windowedComparisonsPerPhyPartMapper());
    }

}
