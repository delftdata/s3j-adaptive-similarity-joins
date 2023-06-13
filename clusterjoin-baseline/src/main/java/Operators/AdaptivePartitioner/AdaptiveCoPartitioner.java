package Operators.AdaptivePartitioner;

import CustomDataTypes.FinalTuple;
import CustomDataTypes.SPTuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class AdaptiveCoPartitioner extends KeyedCoProcessFunction<Integer, SPTuple, SPTuple, FinalTuple> {
    private AdaptivePartitionerCompanion companion;

    public AdaptiveCoPartitioner(AdaptivePartitionerCompanion companion) {
        this.companion = companion;
    }

    private void collectStats(SPTuple t, Context context) throws Exception {
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
    public void processElement1(SPTuple t, Context context, Collector<FinalTuple> collector) throws Exception {
        companion.assignGroup(t, collector, "left");
//        collectStats(t, context);
    }

    @Override
    public void processElement2(SPTuple t, Context context, Collector<FinalTuple> collector) throws Exception {
        companion.assignGroup(t, collector, "right");
//        collectStats(t, context);
    }

    @Override
    public void open(Configuration config){
        companion.open(config, this);
    }
}
