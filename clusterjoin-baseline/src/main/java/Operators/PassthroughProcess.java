package Operators;

import CustomDataTypes.FinalTuple;
import CustomDataTypes.FinalTupleCJ;
import CustomDataTypes.SPTuple;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class PassthroughProcess extends KeyedProcessFunction<Integer, SPTuple, FinalTupleCJ> {
    @Override
    public void processElement(SPTuple spTuple, Context context, Collector<FinalTupleCJ> collector) throws Exception {
        FinalTupleCJ passthrough = new FinalTupleCJ(spTuple.f0,
                spTuple.f1, spTuple.f2, spTuple.f3, spTuple.f4, spTuple.f5, spTuple.f6, "single");
        collector.collect(passthrough);
    }
}
