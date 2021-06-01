import org.apache.flink.api.common.functions.RichFlatMapFunction;
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
import org.slf4j.LoggerFactory;

public class AdaptivePartitioner extends
        ProcessFunction<Tuple6<Integer,String,Integer,Long,Integer,Double[]>,
                        Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>> {

    Double dist_thresh;
    int keyRange;
    private Logger LOG;
    OutputTag<Tuple3<Long, Integer, Integer>> sideLP;
    OutputTag<Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> sideLCentroids;
    ListState<Tuple7<Integer,Integer,String,Integer,Long,Integer,Double[]>> outliers;
    ListState<Tuple6<Integer,String,Integer,Long,Integer,Double[]>> phyOuters;
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

    @Override
    public void open(Configuration config){
        ListStateDescriptor<Tuple7<Integer, Integer,String,Integer,Long,Integer,Double[]>> outliersDesc =
                new ListStateDescriptor<Tuple7<Integer, Integer, String, Integer, Long, Integer, Double[]>>(
                        "outliers",
                        TypeInformation.of(new TypeHint<Tuple7<Integer, Integer, String, Integer, Long, Integer, Double[]>>() {})
                );
        outliers = getRuntimeContext().getListState(outliersDesc);

        ListStateDescriptor<Tuple6<Integer,String,Integer,Long,Integer,Double[]>> phyOutersDesc =
                new ListStateDescriptor<Tuple6<Integer, String, Integer, Long, Integer, Double[]>>(
                        "phyOuters",
                        TypeInformation.of(new TypeHint<Tuple6<Integer, String, Integer, Long, Integer, Double[]>>() {})
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
    public void processElement(Tuple6<Integer, String, Integer, Long, Integer, Double[]> t,
                               Context context,
                               Collector<Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer>> collector)
            throws Exception {

        Double[] emb = t.f5;
        int part_num = Iterables.size(mappingGroupsToNodes.keys());

        PriorityQueue<Tuple2<Integer, Double>> distances =
                new PriorityQueue<Tuple2<Integer, Double>>(new CustomComparator());

        boolean isOutlier = false;

        if (t.f1.equals("pOuter")) {
            for (Map.Entry<Integer, Tuple2<Tuple3<Long, Integer, Double[]>,Integer>> centroid : mappingGroupsToNodes.entries()) {
                Double dist = SimilarityJoinsUtil.AngularDistance(emb, centroid.getValue().f0.f2);
                distances.add(new Tuple2<>(centroid.getKey(), dist));

                if (dist <= 2 * dist_thresh) {
                    collector.collect(new Tuple10<>(centroid.getKey(), "outer", t.f0, t.f1, t.f2, -1, t.f3, t.f4, t.f5, mappingGroupsToNodes.get(centroid.getKey()).f1));
//                    LOG.info(new Tuple9<>(centroid.getKey(), "outer", t.f0, t.f1, t.f2, -1, t.f3, t.f4, t.f5).toString());
                }
            }
            phyOuters.add(t);
        }
        else if (t.f1.equals("pInner")) {
            if (part_num == 0) {
                mappingGroupsToNodes.put(1, new Tuple2<>(new Tuple3<Long, Integer, Double[]>(t.f3, t.f4, emb), t.f0));
                collector.collect(new Tuple10<>(1, "inner", t.f0, t.f1, t.f2, 1, t.f3, t.f4, t.f5, mappingGroupsToNodes.get(1).f1));
//                LOG.info(new Tuple9<>(1, "inner", t.f0, t.f1, t.f2, 1,t.f3, t.f4, t.f5).toString());
                for(Tuple6<Integer,String,Integer,Long,Integer,Double[]> po : phyOuters.get()){
                    Double[] temp = po.f5;
                    if(SimilarityJoinsUtil.AngularDistance(emb, temp) <= 2 * dist_thresh){
                        collector.collect(new Tuple10<>(1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5, mappingGroupsToNodes.get(1).f1));
//                        LOG.info(new Tuple9<>(1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5).toString());
                    }
                }
            } else {
                int inner;
                for (Map.Entry<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> centroid : mappingGroupsToNodes.entries()) {
                    Double dist = SimilarityJoinsUtil.AngularDistance(emb, centroid.getValue().f0.f2);
                    distances.add(new Tuple2<>(centroid.getKey(), dist));

                    if (dist <= 0.5 * dist_thresh) {
                        collector.collect(new Tuple10<>(centroid.getKey(), "inner", t.f0, t.f1, t.f2, centroid.getKey(), t.f3, t.f4, t.f5, mappingGroupsToNodes.get(centroid.getKey()).f1));
//                        LOG.info(new Tuple9<>(centroid.getKey(), "inner", t.f0, t.f1, t.f2, centroid.getKey(), t.f3, t.f4, t.f5).toString());
                        inner = centroid.getKey();
                    }
                }

                try {
                    if (distances.peek().f1 > dist_thresh) {
                        inner = part_num + 1;
                        mappingGroupsToNodes.put(part_num + 1, new Tuple2<>(new Tuple3<>(t.f3, t.f4, emb), t.f0));
                        collector.collect(new Tuple10<>(part_num + 1, "inner", t.f0, t.f1, t.f2, part_num + 1, t.f3, t.f4, t.f5, mappingGroupsToNodes.get(part_num+1).f1));
//                        LOG.info(new Tuple9<>(part_num + 1, "inner", t.f0, t.f1, t.f2, part_num + 1, t.f3, t.f4, t.f5).toString());
                        for(Tuple6<Integer,String,Integer,Long,Integer,Double[]> po : phyOuters.get()){
                            Double[] temp = po.f5;

                            if(SimilarityJoinsUtil.AngularDistance(emb, temp) <= 2 * dist_thresh){
                                collector.collect(new Tuple10<>(part_num + 1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5, mappingGroupsToNodes.get(part_num+1).f1));
//                                LOG.info(new Tuple9<>(part_num + 1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5).toString());
                            }
                        }

                    } else {
                        if (distances.peek().f1 > 0.5 * dist_thresh) {
                            inner = distances.peek().f0;
                            outliers.add(new Tuple7<>(inner,t.f0, t.f1, t.f2, t.f3, t.f4, t.f5));
                            isOutlier = true;
                            collector.collect(
                                    new Tuple10<>(distances.peek().f0, "outlier", t.f0, t.f1, t.f2, distances.peek().f0, t.f3, t.f4, t.f5, mappingGroupsToNodes.get(distances.peek().f0).f1));
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
                                collector.collect(new Tuple10<>(temp.f0, "outer", t.f0, t.f1, t.f2, inner, t.f3, t.f4, t.f5, mappingGroupsToNodes.get(temp.f0).f1));
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

        context.output(sideLP, new Tuple3<Long, Integer, Integer>(t.f3, t.f0, Iterables.size(mappingGroupsToNodes.keys())));

        HashMap<Integer, Tuple3<Long,Integer,Double[]>> partitions = new HashMap<>();
        for(Map.Entry<Integer, Tuple2<Tuple3<Long, Integer, Double[]>,Integer>> centroid : mappingGroupsToNodes.entries()){
            partitions.put(centroid.getKey(), centroid.getValue().f0);
        }
        context.output(sideLCentroids, new Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>(t.f0, partitions));
    }
}