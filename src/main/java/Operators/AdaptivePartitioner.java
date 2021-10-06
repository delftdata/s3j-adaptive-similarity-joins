package Operators;

import CustomDataTypes.SPTuple;
import Utils.CustomComparator;
import CustomDataTypes.FinalTuple;
import Utils.SimilarityJoinsUtil;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;

public class AdaptivePartitioner extends
        ProcessFunction<SPTuple,
                FinalTuple> {

    Double dist_thresh;
    int keyRange;
    private Logger LOG;
    OutputTag<Tuple3<Long, Integer, Integer>> sideLP;
    OutputTag<Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> sideLCentroids;
    ListState<SPTuple> phyOuters;
    MapState<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> mappingGroupsToNodes;

    public AdaptivePartitioner(Double dist_thresh,
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


    // Initialize state
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
    public void processElement(SPTuple t,
                               Context context,
                               Collector<FinalTuple> collector)
            throws Exception {

        Double[] emb = t.f6;
        int part_num = Iterables.size(mappingGroupsToNodes.keys());

        PriorityQueue<Tuple2<Integer, Double>> distances =
                new PriorityQueue<Tuple2<Integer, Double>>(new CustomComparator());

        boolean isOutlier = false;

        // Tuples belonging to the outer partition of the machine can only be used for outer groups.
        // For a tuple to belong to an outer group, it must satisfy a distance criterion.
        if (t.f1.equals("pOuter")) {
            for (Map.Entry<Integer, Tuple2<Tuple3<Long, Integer, Double[]>,Integer>> centroid : mappingGroupsToNodes.entries()) {
                Double dist = SimilarityJoinsUtil.AngularDistance(emb, centroid.getValue().f0.f2);
                distances.add(new Tuple2<>(centroid.getKey(), dist));

                if (dist <= 2 * dist_thresh) {
                    collector.collect(new FinalTuple(centroid.getKey(), "outer", t.f0, t.f1, t.f2, -1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(centroid.getKey()).f1));
//                    LOG.info(new Tuple9<>(centroid.getKey(), "outer", t.f0, t.f1, t.f2, -1, t.f3, t.f4, t.f5).toString());
                }
            }
            phyOuters.add(t);
        }
        // Tuples belonging to the inner partition of the machine can only be used for outer groups.
        // For a tuple to belong to an outer group, it must satisfy a distance criterion.
        else if (t.f1.equals("pInner")) {
            //if there are no groups created yet, create the first pair of outer-inner groups
            // and check if there are any outer tuples that might need to be included to the outer group.
            if (part_num == 0) {
                mappingGroupsToNodes.put(1, new Tuple2<>(new Tuple3<Long, Integer, Double[]>(t.f3, t.f5, emb), t.f0));
                collector.collect(new FinalTuple(1, "inner", t.f0, t.f1, t.f2, 1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(1).f1));
//                LOG.info(new Tuple9<>(1, "inner", t.f0, t.f1, t.f2, 1,t.f3, t.f4, t.f5).toString());
                for(SPTuple po : phyOuters.get()){
                    Double[] temp = po.f6;
                    if(SimilarityJoinsUtil.AngularDistance(emb, temp) <= 2 * dist_thresh){
                        collector.collect(new FinalTuple(1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5, po.f6, mappingGroupsToNodes.get(1).f1));
//                        LOG.info(new Tuple9<>(1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5).toString());
                    }
                }
            } else {
                // if there are groups, calculate the distance of the incoming tuple t from their centroids.
                // Based on these distances decide whether there is an existing group that can contain t, whether
                // a new pair of groups with t as its centroid must be created, or t must be an outlier.
                // Based on the calculated distances and a routing criterion, we also decide to which groups t should be
                // included as an outer.
                int inner;
                for (Map.Entry<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> centroid : mappingGroupsToNodes.entries()) {
                    Double dist = SimilarityJoinsUtil.AngularDistance(emb, centroid.getValue().f0.f2);
                    distances.add(new Tuple2<>(centroid.getKey(), dist));

                    if (dist <= 0.5 * dist_thresh) {
                        collector.collect(new FinalTuple(centroid.getKey(), "inner", t.f0, t.f1, t.f2, centroid.getKey(), t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(centroid.getKey()).f1));
//                        LOG.info(new Tuple9<>(centroid.getKey(), "inner", t.f0, t.f1, t.f2, centroid.getKey(), t.f3, t.f4, t.f5).toString());
                        inner = centroid.getKey();
                    }
                }

                try {
                    if (distances.peek().f1 > dist_thresh) {
                        inner = part_num + 1;
                        mappingGroupsToNodes.put(part_num + 1, new Tuple2<>(new Tuple3<>(t.f3, t.f5, emb), t.f0));
                        collector.collect(new FinalTuple(part_num + 1, "inner", t.f0, t.f1, t.f2, part_num + 1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(part_num+1).f1));
//                        LOG.info(new Tuple9<>(part_num + 1, "inner", t.f0, t.f1, t.f2, part_num + 1, t.f3, t.f4, t.f5).toString());
                        for(SPTuple po : phyOuters.get()){
                            Double[] temp = po.f6;

                            if(SimilarityJoinsUtil.AngularDistance(emb, temp) <= 2 * dist_thresh){
                                collector.collect(new FinalTuple(part_num + 1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5, po.f6, mappingGroupsToNodes.get(part_num+1).f1));
//                                LOG.info(new Tuple9<>(part_num + 1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5).toString());
                            }
                        }

                    } else {
                        if (distances.peek().f1 > 0.5 * dist_thresh) {
                            inner = distances.peek().f0;
                            isOutlier = true;
                            collector.collect(
                                    new FinalTuple(distances.peek().f0, "outlier", t.f0, t.f1, t.f2, distances.peek().f0, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(distances.peek().f0).f1));
//                            LOG.info(new Tuple9<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[]>(distances.peek().f0, "outlier", t.f0, t.f1, t.f2, distances.peek().f0, t.f3, t.f4, t.f5).toString());
                        } else {
                            inner = distances.peek().f0;
                        }
                    }

                    while (!distances.isEmpty()) {
                        Tuple2<Integer, Double> temp = distances.poll();
                        if (temp.f0 == inner) {
                            continue;
                        } else if ((temp.f1 > 2 * dist_thresh)) {
                            break;
                        } else {
                            if (inner > temp.f0) {
                                collector.collect(new FinalTuple(temp.f0, "outer", t.f0, t.f1, t.f2, inner, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(temp.f0).f1));
//                                LOG.info(new Tuple9<>(temp.f0, "outer", t.f0, t.f1, t.f2, inner, t.f3, t.f4, t.f5).toString());
                            }

                        }
                    }
                } catch (Exception e) {
                    System.out.println("oups:" + e.getMessage() + " on " + (distances.peek().f0 % 128) / keyRange);
                    throw e;
                }
            }
        }
        // side output to get the tuples emitted by the operator (for statistics)
        context.output(sideLP, new Tuple3<Long, Integer, Integer>(t.f3, t.f0, Iterables.size(mappingGroupsToNodes.keys())));

        //side output to get the group centroids
        HashMap<Integer, Tuple3<Long,Integer,Double[]>> partitions = new HashMap<>();
        for(Map.Entry<Integer, Tuple2<Tuple3<Long, Integer, Double[]>,Integer>> centroid : mappingGroupsToNodes.entries()){
            partitions.put(centroid.getKey(), centroid.getValue().f0);
        }
        context.output(sideLCentroids, new Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>(t.f0, partitions));
    }
}