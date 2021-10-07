package Operators;

import CustomDataTypes.SPTuple;
import Utils.CustomComparator;
import CustomDataTypes.FinalTuple;
import Utils.SimilarityJoinsUtil;
import Utils.StreamProcessing;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;

public class AdaptiveCoPartitioner extends
        KeyedCoProcessFunction<Integer,
                SPTuple,
                SPTuple,
                FinalTuple> {

    Double dist_thresh;
    int keyRange;
    private Logger LOG;
    OutputTag<Tuple3<Long, Integer, Integer>> sideLP;
    OutputTag<Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> sideLCentroids;
    ListState<SPTuple> phyOuters;
    MapState<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> mappingGroupsToNodes;

    public AdaptiveCoPartitioner(Double dist_thresh,
                               int keyRange,
                               Logger LOG,
                               OutputTag<Tuple3<Long, Integer, Integer>> sideLP,
                               OutputTag<Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> sideLCentroids
    ) throws Exception{
        this.dist_thresh = dist_thresh;
        this.keyRange = keyRange;
        this.LOG = LOG;
        this.sideLP = sideLP;
        this.sideLCentroids = sideLCentroids;
    }

    @Override
    public void open(Configuration config){
        ListStateDescriptor<SPTuple> phyOutersDesc =
                new ListStateDescriptor<SPTuple>(
                        "phyOuters",
                        TypeInformation.of(SPTuple.class)
                );
        phyOuters = getRuntimeContext().getListState(phyOutersDesc);

        MapStateDescriptor<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> mappingGroupsToNodesDesc =
                new MapStateDescriptor<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>>(
                        "mapping",
                        TypeInformation.of(new TypeHint<Integer>() {}),
                        TypeInformation.of(new TypeHint<Tuple2<Tuple3<Long,Integer,Double[]>, Integer>>() {})
                );
        mappingGroupsToNodes = getRuntimeContext().getMapState(mappingGroupsToNodesDesc);
    }


    @Override
    public void processElement1(SPTuple t,
                                Context context,
                                Collector<FinalTuple> collector)
            throws Exception {
        StreamProcessing.AssignGroup(t, collector, mappingGroupsToNodes, dist_thresh, phyOuters, keyRange, "left");

        context.output(sideLP, new Tuple3<Long, Integer, Integer>(t.f3, t.f0, Iterables.size(mappingGroupsToNodes.keys())));

        HashMap<Integer, Tuple3<Long,Integer,Double[]>> partitions = new HashMap<>();
        for(Map.Entry<Integer, Tuple2<Tuple3<Long, Integer, Double[]>,Integer>> centroid : mappingGroupsToNodes.entries()){
            partitions.put(centroid.getKey(), centroid.getValue().f0);
        }
        context.output(sideLCentroids, new Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>(t.f0, partitions));
    }

    @Override
    public void processElement2(SPTuple t,
                                Context context,
                                Collector<FinalTuple> collector)
            throws Exception {
        StreamProcessing.AssignGroup(t, collector, mappingGroupsToNodes, dist_thresh, phyOuters, keyRange, "right");

        context.output(sideLP, new Tuple3<Long, Integer, Integer>(t.f3, t.f0, Iterables.size(mappingGroupsToNodes.keys())));

        HashMap<Integer, Tuple3<Long,Integer,Double[]>> partitions = new HashMap<>();
        for(Map.Entry<Integer, Tuple2<Tuple3<Long, Integer, Double[]>,Integer>> centroid : mappingGroupsToNodes.entries()){
            partitions.put(centroid.getKey(), centroid.getValue().f0);
        }
        context.output(sideLCentroids, new Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>(t.f0, partitions));
    }

}