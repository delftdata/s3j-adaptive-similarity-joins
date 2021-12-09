package Statistics.Latency;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AddWindowStartTime extends ProcessWindowFunction<
        Tuple2<Integer, Long>,
        Tuple2<Long, Tuple2<Integer,Long>>,
        Integer,
        TimeWindow> {
    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<Tuple2<Integer, Long>> iterable,
                        Collector<Tuple2<Long, Tuple2<Integer, Long>>> collector)
            throws Exception {

        for(Tuple2<Integer, Long> item : iterable){
            collector.collect(new Tuple2<>(context.window().getStart(), item));
        }

    }
}
