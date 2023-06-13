package Statistics.FinalComputations;

import CustomDataTypes.ShortOutput;
import org.apache.flink.api.common.functions.ReduceFunction;

public class FinalComputationsReduce implements ReduceFunction<ShortOutput> {
    @Override
    public ShortOutput reduce(ShortOutput t1, ShortOutput t2) throws Exception {
        return new ShortOutput(t1.f0, t1.f1, t1.f2+t2.f2);
    }
}
