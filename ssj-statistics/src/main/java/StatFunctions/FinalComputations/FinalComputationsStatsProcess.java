package StatFunctions.FinalComputations;

import CustomDataTypes.ShortOutput;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class FinalComputationsStatsProcess extends ProcessWindowFunction<
        ShortOutput, ShortOutput, Integer, TimeWindow> {


    @Override
    public void process(Integer key,
                        ProcessWindowFunction<ShortOutput, ShortOutput, Integer, TimeWindow>.Context context,
                        Iterable<ShortOutput> iterable,
                        Collector<ShortOutput> collector) throws Exception {
        ShortOutput input = iterable.iterator().next();
        collector.collect(new ShortOutput(context.window().getStart(), input.f1, input.f2));

    }
}
