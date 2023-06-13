package Statistics.GroupLevelFinalComputations;

import CustomDataTypes.GroupLevelShortOutput;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class GroupLevelFinalComputationsStatsProcess extends ProcessWindowFunction<
        GroupLevelShortOutput,
        GroupLevelShortOutput,
        Tuple3<Integer, Integer, Integer>,
        TimeWindow
        >{
    @Override
    public void process(Tuple3<Integer, Integer, Integer> key,
                        Context context,
                        Iterable<GroupLevelShortOutput> iterable,
                        Collector<GroupLevelShortOutput> collector) throws Exception {
        GroupLevelShortOutput input = iterable.iterator().next();
        collector.collect(new GroupLevelShortOutput(context.window().getStart(), input.f1, input.f2, input.f3, input.f4, input.f5));
    }
}
