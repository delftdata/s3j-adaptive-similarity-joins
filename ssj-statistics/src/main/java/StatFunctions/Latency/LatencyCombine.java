package StatFunctions.Latency;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class LatencyCombine extends ProcessAllWindowFunction<
        Tuple3<Integer, Long, Long>,
        Tuple2<Long, List<Tuple3<Integer, Long, Long>>>,
        TimeWindow
        > {
    @Override
    public void process(Context context,
                        Iterable<Tuple3<Integer, Long, Long>> iterable,
                        Collector<Tuple2<Long, List<Tuple3<Integer, Long, Long>>>> collector)
            throws Exception {
        List<Tuple3<Integer, Long, Long>> combined = new ArrayList<>();
        for (Tuple3<Integer,Long, Long> tmp : iterable){
            combined.add(tmp);
        }
        collector.collect(new Tuple2<>(context.window().getStart(), combined));
    }
}
