package Utils;

import CustomDataTypes.FinalTuple;
import CustomDataTypes.SPTuple;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.PriorityQueue;

public class StreamProcessing {
    public static void AssignGroup(
            SPTuple t,
            Collector<FinalTuple> collector,
            MapState<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> mappingGroupsToNodes,
            Double dist_thresh,
            ListState<SPTuple> phyOuters,
            int keyRange,
            String origin) throws Exception  {

        Double[] emb = t.f6;
        String originStream = origin;
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
                    collector.collect(new FinalTuple(centroid.getKey(), "outer", t.f0, t.f1, t.f2, -1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(centroid.getKey()).f1, originStream));
//                    LOG.warn(new FinalTuple(centroid.getKey(), "outer", t.f0, t.f1, t.f2, -1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(centroid.getKey()).f1, originStream).toString());
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
                collector.collect(new FinalTuple(1, "inner", t.f0, t.f1, t.f2, 1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(1).f1, originStream));
//                LOG.warn(new FinalTuple(1, "inner", t.f0, t.f1, t.f2, 1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(1).f1, originStream).toString());
                for(SPTuple po : phyOuters.get()){
                    Double[] temp = po.f6;
                    if(SimilarityJoinsUtil.AngularDistance(emb, temp) <= 2 * dist_thresh){
                        collector.collect(new FinalTuple(1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5, po.f6, mappingGroupsToNodes.get(1).f1, originStream));
//                        LOG.warn(new FinalTuple(1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5, po.f6, mappingGroupsToNodes.get(1).f1, originStream).toString());
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
                        collector.collect(new FinalTuple(centroid.getKey(), "inner", t.f0, t.f1, t.f2, centroid.getKey(), t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(centroid.getKey()).f1, originStream));
//                        LOG.warn(new FinalTuple(centroid.getKey(), "inner", t.f0, t.f1, t.f2, centroid.getKey(), t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(centroid.getKey()).f1, originStream).toString());
                        inner = centroid.getKey();
                    }
                }

                try {
                    if (distances.peek().f1 > dist_thresh) {
                        inner = part_num + 1;
                        mappingGroupsToNodes.put(part_num + 1, new Tuple2<>(new Tuple3<>(t.f3, t.f5, emb), t.f0));
                        collector.collect(new FinalTuple(part_num + 1, "inner", t.f0, t.f1, t.f2, part_num + 1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(part_num+1).f1, originStream));
//                        LOG.warn(new FinalTuple(part_num + 1, "inner", t.f0, t.f1, t.f2, part_num + 1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(part_num+1).f1, originStream).toString());
                        for(SPTuple po : phyOuters.get()){
                            Double[] temp = po.f6;

                            if(SimilarityJoinsUtil.AngularDistance(emb, temp) <= 2 * dist_thresh){
                                collector.collect(new FinalTuple(part_num + 1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5, po.f6, mappingGroupsToNodes.get(part_num+1).f1, originStream));
//                                LOG.warn(new FinalTuple(part_num + 1, "inner", t.f0, t.f1, t.f2, part_num + 1, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(part_num+1).f1, originStream).toString());
                            }
                        }

                    } else {
                        if (distances.peek().f1 > 0.5 * dist_thresh) {
                            inner = distances.peek().f0;
                            isOutlier = true;
                            collector.collect(
                                    new FinalTuple(distances.peek().f0, "outlier", t.f0, t.f1, t.f2, distances.peek().f0, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(distances.peek().f0).f1, originStream));
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
                                collector.collect(new FinalTuple(temp.f0, "outer", t.f0, t.f1, t.f2, inner, t.f3, t.f4, t.f5, t.f6, mappingGroupsToNodes.get(temp.f0).f1, originStream));
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
