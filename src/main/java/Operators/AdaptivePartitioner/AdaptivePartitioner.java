package Operators.AdaptivePartitioner;

import CustomDataTypes.FinalTuple;
import CustomDataTypes.JointTuple;
import CustomDataTypes.SPTuple;
import Utils.CleanPhyOuters;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class AdaptivePartitioner extends KeyedBroadcastProcessFunction<Integer, JointTuple, Integer, FinalTuple> {
    private AdaptivePartitionerCompanion companion;

    public AdaptivePartitioner(AdaptivePartitionerCompanion companion) {
        this.companion = companion;
    }

    private void collectStats(JointTuple t, Context context) throws Exception {
        // side output to get the tuples emitted by the operator (for statistics)
        if (companion.getSideLPartitions() != null) {
            context.output(companion.getSideLPartitions() , new Tuple3<>(t.f3, t.f0, Iterables.size(companion.getMappingGroupsToNodes() .keys())));
        }

        //side output to get the group centroids
        if (companion.getSideLCentroids() != null) {
            context.output(companion.getSideLCentroids(), new Tuple2<>(t.f0, companion.getCentroidStats()));
        }
    }

    @Override
    public void processElement(JointTuple t, ReadOnlyContext context, Collector<FinalTuple> collector) throws Exception {
        SPTuple sp = new SPTuple(t.f0, t.f1, t.f2, t.f3, t.f4, t.f5, t.f6);
        companion.assignGroup(sp, collector, t.f7);
    }

    @Override
    public void processBroadcastElement(Integer integer, Context ctx, Collector<FinalTuple> collector) throws Exception{
        ctx.applyToKeyedState(companion.getPhyOutersDesc(), new CleanPhyOuters());
    }

    @Override
    public void open(Configuration config){
        companion.open(config, this);
    }
}
