package StatFunctions.Latency;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class LatencyProcess extends ProcessWindowFunction<
        Tuple2<Long, Long>,
        Tuple3<Integer, Long, Long>,
        Integer,
        TimeWindow
        > {


    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<Tuple2<Long, Long>> iterable,
                        Collector<Tuple3<Integer, Long, Long>> collector)
            throws Exception {
        Tuple2<Long, Long> averageTuple = iterable.iterator().next();
        collector.collect(new Tuple3<>(key, averageTuple.f0, averageTuple.f1));
    }
}
