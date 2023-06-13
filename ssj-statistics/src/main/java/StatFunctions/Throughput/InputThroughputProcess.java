package StatFunctions.Throughput;

import CustomDataTypes.GroupLevelShortOutput;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class InputThroughputProcess extends ProcessAllWindowFunction<
        Tuple2<Long, Long>,
        Tuple2<Long, Long>,
        TimeWindow> {

    @Override
    public void process(Context context,
                        Iterable<Tuple2<Long, Long>> iterable,
                        Collector<Tuple2<Long, Long>> collector) throws Exception {
        Tuple2<Long, Long> input = iterable.iterator().next();
        Long windowLength = (context.window().getEnd() - context.window().getStart())/1000;
        collector.collect(new Tuple2<>(context.window().getStart(), input.f1/windowLength));
    }

}
