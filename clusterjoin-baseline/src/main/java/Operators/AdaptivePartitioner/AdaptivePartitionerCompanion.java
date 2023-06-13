package Operators.AdaptivePartitioner;

import CustomDataTypes.FinalTuple;
import CustomDataTypes.SPTuple;
import Utils.CustomComparator;
import Utils.SimilarityJoinsUtil;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class AdaptivePartitionerCompanion implements Serializable {
    private double dist_thresh;
    private int keyRange;
    private ListState<Tuple8<Integer,String,Integer,Long, Long,Integer,Double[],String>> phyOuters;
    private MapState<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> mappingGroupsToNodes;
    private OutputTag<Tuple2<Integer, HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> sideLCentroids;
    private OutputTag<Tuple3<Long, Integer, Integer>> sideLPartitions;
    private Logger LOG;


    public AdaptivePartitionerCompanion() {
    }

    public double getDist_thresh() {
        return dist_thresh;
    }

    public int getKeyRange() {
        return keyRange;
    }

    public ListState<Tuple8<Integer,String,Integer,Long, Long,Integer,Double[],String>> getPhyOuters() {
        return phyOuters;
    }

    public MapState<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> getMappingGroupsToNodes() {
        return mappingGroupsToNodes;
    }

    public OutputTag<Tuple2<Integer, HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> getSideLCentroids() {
        return sideLCentroids;
    }

    public OutputTag<Tuple3<Long, Integer, Integer>> getSideLPartitions() {
        return sideLPartitions;
    }

    public void setSideLCentroids(OutputTag<Tuple2<Integer, HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> sideLCentroids) {
        this.sideLCentroids = sideLCentroids;
    }

    public void setSideLPartitions(OutputTag<Tuple3<Long, Integer, Integer>> sideLPartitions) {
        this.sideLPartitions = sideLPartitions;
    }

    public AdaptivePartitionerCompanion(double dist_thresh, int keyRange) {
        this.dist_thresh = dist_thresh;
        this.keyRange = keyRange;
        this.LOG = LoggerFactory.getLogger(this.getClass().getName());
    }

    void open(Configuration config, AbstractRichFunction adaptivePartitioner){
        ListStateDescriptor<Tuple8<Integer,String,Integer,Long, Long,Integer,Double[],String>> phyOutersDesc =
                new ListStateDescriptor<Tuple8<Integer,String,Integer,Long, Long,Integer,Double[],String>>(
                        "phyOuters",
                        TypeInformation.of(new TypeHint<Tuple8<Integer, String, Integer, Long, Long, Integer, Double[], String>>() {})
                );
        phyOuters = adaptivePartitioner.getRuntimeContext().getListState(phyOutersDesc);

        MapStateDescriptor<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> mappingGroupsToNodesDesc =
                new MapStateDescriptor<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>>(
                        "mapping",
                        TypeInformation.of(new TypeHint<Integer>() {}),
                        TypeInformation.of(new TypeHint<Tuple2<Tuple3<Long,Integer,Double[]>, Integer>>() {})
                );
        mappingGroupsToNodes = adaptivePartitioner.getRuntimeContext().getMapState(mappingGroupsToNodesDesc);
    }

    HashMap<Integer, Tuple3<Long,Integer,Double[]>> getCentroidStats() throws Exception {
        HashMap<Integer, Tuple3<Long,Integer,Double[]>> partitions = new HashMap<>();
        for(Map.Entry<Integer, Tuple2<Tuple3<Long, Integer, Double[]>,Integer>> centroid : mappingGroupsToNodes.entries()){
            partitions.put(centroid.getKey(), centroid.getValue().f0);
        }
        return partitions;
    }

    void assignGroup(
            SPTuple t,
            Collector<FinalTuple> collector,
            String origin) throws Exception {
        Double[] emb = t.f6;
        int part_num = Iterables.size(mappingGroupsToNodes.keys());

        PriorityQueue<Tuple2<Integer, Double>> distances =
                new PriorityQueue<Tuple2<Integer, Double>>(new CustomComparator());

        boolean isOutlier = false;

        if (t.f1.equals("pOuter")) {
            for (Map.Entry<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> centroid : mappingGroupsToNodes.entries()) {
                Double dist = SimilarityJoinsUtil.AngularDistance(emb, centroid.getValue().f0.f2);
                distances.add(new Tuple2<>(centroid.getKey(), dist));

                if (dist <= 2 * dist_thresh) {
                    collector.collect(new FinalTuple(centroid.getKey(), "outer", t.f0, t.f1, t.f2, -1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(centroid.getKey()).f1, origin));
//                    LOG.warn(new FinalTuple(centroid.getKey(), "outer", t.f0, t.f1, t.f2, -1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(centroid.getKey()).f1, originStream).toString());
                }
            }
            phyOuters.add(new Tuple8<>(t.f0, t.f1, t.f2, t.f3, t.f4, t.f5, t.f6, origin));
        } else if (t.f1.equals("pInner")) {
            if (part_num == 0) {
                mappingGroupsToNodes.put(1, new Tuple2<>(new Tuple3<Long, Integer, Double[]>(t.f3, t.f5, emb), t.f0));
                collector.collect(new FinalTuple(1, "inner", t.f0, t.f1, t.f2, 1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(1).f1, origin));
                LOG.warn(new FinalTuple(1, "inner", t.f0, t.f1, t.f2, 1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(1).f1, origin).toString());
                for (Tuple8<Integer, String, Integer, Long, Long, Integer, Double[], String> po : phyOuters.get()) {
                    Double[] temp = po.f6;
                    if (SimilarityJoinsUtil.AngularDistance(emb, temp) <= 2 * dist_thresh) {
                        collector.collect(new FinalTuple(1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5, po.f6, mappingGroupsToNodes.get(1).f1, po.f7));
//                        LOG.warn(new FinalTuple(1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5, po.f6, mappingGroupsToNodes.get(1).f1, po.f7).toString());
                    }
                }
            } else {
                int inner;
                for (Map.Entry<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> centroid : mappingGroupsToNodes.entries()) {
                    Double dist = SimilarityJoinsUtil.AngularDistance(emb, centroid.getValue().f0.f2);
                    distances.add(new Tuple2<>(centroid.getKey(), dist));

                    if (dist <= 0.5 * dist_thresh) {
                        collector.collect(new FinalTuple(centroid.getKey(), "inner", t.f0, t.f1, t.f2, centroid.getKey(), t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(centroid.getKey()).f1, origin));
//                        LOG.warn(new FinalTuple(centroid.getKey(), "inner", t.f0, t.f1, t.f2, centroid.getKey(), t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(centroid.getKey()).f1, originStream).toString());
                        inner = centroid.getKey();
                    }
                }

                try {
                    if (distances.peek().f1 > dist_thresh) {
                        inner = part_num + 1;
                        mappingGroupsToNodes.put(part_num + 1, new Tuple2<>(new Tuple3<>(t.f3, t.f5, emb), t.f0));
                        collector.collect(new FinalTuple(part_num + 1, "inner", t.f0, t.f1, t.f2, part_num + 1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(part_num + 1).f1, origin));
//                        LOG.warn(new FinalTuple(part_num + 1, "inner", t.f0, t.f1, t.f2, part_num + 1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(part_num+1).f1, originStream).toString());
                        for (Tuple8<Integer, String, Integer, Long, Long, Integer, Double[], String> po : phyOuters.get()) {
                            Double[] temp = po.f6;

                            if (SimilarityJoinsUtil.AngularDistance(emb, temp) <= 2 * dist_thresh) {
                                collector.collect(new FinalTuple(part_num + 1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5, po.f6, mappingGroupsToNodes.get(part_num + 1).f1, po.f7));
//                                LOG.warn(new FinalTuple(part_num + 1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5, po.f6, mappingGroupsToNodes.get(part_num+1).f1, po.f7).toString());
                            }
                        }

                    } else {
                        if (distances.peek().f1 > 0.5 * dist_thresh) {
                            inner = distances.peek().f0;
                            isOutlier = true;
                            collector.collect(
                                    new FinalTuple(distances.peek().f0, "outlier", t.f0, t.f1, t.f2, distances.peek().f0, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(distances.peek().f0).f1, origin));
//                            LOG.warn(new FinalTuple(distances.peek().f0, "outlier", t.f0, t.f1, t.f2, distances.peek().f0, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(distances.peek().f0).f1, originStream).toString());
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
                                collector.collect(new FinalTuple(temp.f0, "outer", t.f0, t.f1, t.f2, inner, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(temp.f0).f1, origin));
//                                LOG.warn(new FinalTuple(temp.f0, "outer", t.f0, t.f1, t.f2, inner, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(temp.f0).f1, originStream).toString());
                            }

                        }
                    }
                } catch (Exception e) {
                    System.out.println("oups:" + e.getMessage() + " on " + (distances.peek().f0 % 128) / keyRange);
                    throw e;
                }
            }
        }

    }
}
