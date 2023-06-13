package Operators;

import CustomDataTypes.FinalTupleCJ;
import CustomDataTypes.SPTuple;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class PassthroughCoProcess extends KeyedCoProcessFunction<Integer, SPTuple, SPTuple, FinalTupleCJ> {

    @Override
    public void processElement1(SPTuple spTuple, Context context, Collector<FinalTupleCJ> collector) throws Exception {
        FinalTupleCJ passthrough = new FinalTupleCJ(spTuple.f0,
                spTuple.f1, spTuple.f2, spTuple.f3, spTuple.f4, spTuple.f5, spTuple.f6, "left");
        collector.collect(passthrough);
    }

    @Override
    public void processElement2(SPTuple spTuple, Context context, Collector<FinalTupleCJ> collector) throws Exception {
        FinalTupleCJ passthrough = new FinalTupleCJ(spTuple.f0,
                spTuple.f1, spTuple.f2, spTuple.f3, spTuple.f4, spTuple.f5, spTuple.f6, "right");
        collector.collect(passthrough);
    }
}
