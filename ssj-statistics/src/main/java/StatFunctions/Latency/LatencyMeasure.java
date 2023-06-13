package StatFunctions.Latency;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class LatencyMeasure extends KeyedProcessFunction<
        Integer,
        Tuple3<Integer, Long, Long>,
        Tuple2<Integer, Long>> {

    @Override
    public void processElement(
            Tuple3<Integer, Long, Long> item,
            Context context,
            Collector<Tuple2<Integer, Long>> collector)
        throws Exception {

//        System.out.println(context.timestamp());
        Long start_time;
        collector.collect(
                new Tuple2<Integer, Long>(
                            item.f0, item.f2 - item.f1));
    }
}
