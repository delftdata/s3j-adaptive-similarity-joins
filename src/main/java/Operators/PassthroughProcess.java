package Operators;

import CustomDataTypes.JointTuple;
import CustomDataTypes.SPTuple;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class PassthroughProcess extends KeyedProcessFunction<Integer, SPTuple, JointTuple> {
    @Override
    public void processElement(SPTuple spTuple, Context context, Collector<JointTuple> collector) throws Exception {
        JointTuple passthrough = new JointTuple(spTuple, "single");
        collector.collect(passthrough);
    }
}
