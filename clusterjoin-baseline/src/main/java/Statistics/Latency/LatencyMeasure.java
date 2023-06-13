package Statistics.Latency;

import CustomDataTypes.FinalOutput;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class LatencyMeasure extends KeyedProcessFunction<
        Integer,
        Tuple4<Long, Integer, Long, Long>,
        Tuple2<Integer, Long>> {

    @Override
    public void processElement(
            Tuple4<Long, Integer, Long, Long> item,
            Context context,
            Collector<Tuple2<Integer, Long>> collector)
        throws Exception {

        Long start_time;
        if(item.f2 > item.f3){
            start_time = item.f2;
        }
        else{
            start_time = item.f3;
        }
        collector.collect(
                new Tuple2<Integer, Long>(
                            item.f1, item.f0 - start_time));
    }
}
