package Statistics.GroupLevelFinalComputations;

import CustomDataTypes.GroupLevelShortOutput;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GroupLevelCombineProcessFunction extends ProcessAllWindowFunction<
        GroupLevelShortOutput,
        Tuple2<Long, List<Tuple3<Integer, Integer, Long>>>,
        TimeWindow> {
    @Override
    public void process(Context context,
                        Iterable<GroupLevelShortOutput> iterable,
                        Collector<Tuple2<Long, List<Tuple3<Integer, Integer, Long>>>> collector)
            throws Exception {

        List<Tuple3<Integer, Integer, Long>> combined = new ArrayList<>();

        for (GroupLevelShortOutput tmp : iterable){
            combined.add(new Tuple3<>(tmp.f1, tmp.f2, tmp.f3));
        }
        collector.collect(new Tuple2<>(context.window().getStart(), combined));
    }
}
