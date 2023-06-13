package StatFunctions.Throughput;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class InputThroughputReduce implements ReduceFunction<Tuple2<Long, Long>> {

    @Override
    public Tuple2<Long, Long> reduce(Tuple2<Long, Long> t1, Tuple2<Long, Long> t2) throws Exception {
        return new Tuple2<Long, Long>(t1.f0, t1.f1 + t2.f1);
    }

}
