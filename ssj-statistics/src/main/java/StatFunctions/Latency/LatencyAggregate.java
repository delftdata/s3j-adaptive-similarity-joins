package StatFunctions.Latency;

import CustomDataTypes.ShortFinalOutput;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class LatencyAggregate implements AggregateFunction<ShortFinalOutput, Tuple2<Long, Long>, Tuple2<Long, Long>> {
    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return new Tuple2<>(0L, 0L);
    }

    @Override
    public Tuple2<Long, Long> add(ShortFinalOutput in, Tuple2<Long, Long> acc) {
        Long latency = in.f0 - in.f1;
        return new Tuple2<>(acc.f0 + latency, acc.f1 + 1L);
    }

    @Override
    public Tuple2<Long, Long> getResult(Tuple2<Long, Long> acc) {
        return acc;
    }

    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> acc1, Tuple2<Long, Long> acc2) {
        return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
    }
}
