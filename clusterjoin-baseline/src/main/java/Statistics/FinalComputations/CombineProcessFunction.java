package Statistics.FinalComputations;

import CustomDataTypes.ShortOutput;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CombineProcessFunction extends ProcessAllWindowFunction<
        ShortOutput,
        Tuple2<Long, List<Tuple2<Integer, Long>>>,
        TimeWindow> {
    @Override
    public void process(
            ProcessAllWindowFunction<ShortOutput, Tuple2<Long, List<Tuple2<Integer, Long>>>, TimeWindow>.Context context,
            Iterable<ShortOutput> iterable,
            Collector<Tuple2<Long, List<Tuple2<Integer, Long>>>> collector)
            throws Exception {

        List<Tuple2<Integer, Long>> combined = new ArrayList<>();

        for (ShortOutput tmp : iterable) {
            combined.add(new Tuple2<>(tmp.f1, tmp.f2));
        }
        collector.collect(new Tuple2<>(context.window().getStart(), combined));
    }
}
