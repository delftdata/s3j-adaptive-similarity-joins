package Statistics.Latency;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class PercentilesCombine extends ProcessAllWindowFunction<
        Tuple2<Integer, List<Tuple2<String,Long>>>,
        Tuple2<Long, List<Tuple2<Integer, List<Tuple2<String,Long>>>>>,
        TimeWindow> {
    @Override
    public void process(
            Context context,
            Iterable<Tuple2<Integer, List<Tuple2<String, Long>>>> iterable,
            Collector<Tuple2<Long, List<Tuple2<Integer, List<Tuple2<String, Long>>>>>> collector)
            throws Exception {

        List<Tuple2<Integer, List<Tuple2<String,Long>>>> combined = new ArrayList<>();
        for (Tuple2<Integer, List<Tuple2<String,Long>>> tmp : iterable){
            combined.add(tmp);
        }
        collector.collect(new Tuple2<>(context.window().getStart(), combined));

    }
}
