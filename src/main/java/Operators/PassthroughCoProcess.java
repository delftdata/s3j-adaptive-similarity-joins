package Operators;

import CustomDataTypes.JointTuple;
import CustomDataTypes.SPTuple;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class PassthroughCoProcess extends KeyedCoProcessFunction<Integer, SPTuple, SPTuple, JointTuple> {

    @Override
    public void processElement1(SPTuple spTuple, Context context, Collector<JointTuple> collector) throws Exception {
        JointTuple passthrough = new JointTuple(spTuple, "left");
        collector.collect(passthrough);
    }

    @Override
    public void processElement2(SPTuple spTuple, Context context, Collector<JointTuple> collector) throws Exception {
        JointTuple passthrough = new JointTuple(spTuple, "right");
        collector.collect(passthrough);
    }
}
