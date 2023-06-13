package StatFunctions;

import CustomDataTypes.GroupLevelShortOutput;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class GroupLevelCombineLoadEstimations extends ProcessAllWindowFunction<
        Tuple5<Long, Integer, Integer, Integer, Long>,
        Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>,
        TimeWindow> {
    @Override
    public void process(Context context,
                        Iterable<Tuple5<Long,Integer,Integer,Integer,Long>> iterable,
                        Collector<Tuple2<Long, List<Tuple4<Integer, Integer, Integer, Long>>>> collector)
            throws Exception {

        List<Tuple4<Integer, Integer, Integer, Long>> combined = new ArrayList<>();

        for (Tuple5<Long,Integer,Integer,Integer,Long> tmp : iterable){
            combined.add(new Tuple4<>(tmp.f1, tmp.f2, tmp.f3, tmp.f4));
        }
        collector.collect(new Tuple2<>(context.window().getStart(), combined));
    }
}
