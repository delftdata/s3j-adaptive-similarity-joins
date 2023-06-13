package Statistics.Latency;



import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Comparator;

public class LatencyComparator implements Comparator<Tuple2<Integer, Long>> {
    @Override
    public int compare(Tuple2<Integer, Long> o1, Tuple2<Integer, Long> o2) {
        return Long.compare(o1.f1, o2.f1);
    }
}
