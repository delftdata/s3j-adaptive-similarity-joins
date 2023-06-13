package Statistics.Latency;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;

public class Percentiles extends ProcessWindowFunction<
        Tuple2<Integer, Long>,
        Tuple2<Integer, List<Tuple2<String, Long>>>,
        Integer,
        TimeWindow> {

    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<Tuple2<Integer, Long>> iterable,
                        Collector<Tuple2<Integer, List<Tuple2<String,Long>>>> collector)
            throws Exception {

        Long[] latencies =
                StreamSupport
                        .stream(iterable.spliterator(), false)
                        .sorted(new LatencyComparator())
                        .map(t -> t.f1)
                        .toArray(Long[]::new);

        List<Tuple2<String, Long>> percentiles = new ArrayList<>();
        percentiles.add(new Tuple2<>("50%",percentile(latencies, 0.5)));
        percentiles.add(new Tuple2<>("90%",percentile(latencies,0.9)));
        percentiles.add(new Tuple2<>("95%",percentile(latencies,0.95)));
        percentiles.add(new Tuple2<>("99%",percentile(latencies,0.99)));
        percentiles.add(new Tuple2<>("99.9%",percentile(latencies,0.999)));
        collector.collect(new Tuple2<>(key, percentiles));

    }

    public static long percentile(Long[] latencies, double percentile) {
        int index = (int) Math.ceil(percentile * latencies.length) - 1;
        return latencies[index];
    }

}
