import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdaptivePartitioner extends
        RichFlatMapFunction<Tuple6<Integer,String,Integer,Long,Integer,Double[]>,
                Tuple9<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[]>> {

    Double dist_thresh;
    int keyRange;
    private Logger LOG;
    HashMap<Integer, Tuple3<Long, Integer, Double[]>> partitions = new HashMap<>();
    ListState<Tuple7<Integer,Integer,String,Integer,Long,Integer,Double[]>> outliers;
    ListState<Tuple6<Integer,String,Integer,Long,Integer,Double[]>> phyOuters;

    public AdaptivePartitioner(Double dist_thresh, int keyRange, Logger LOG) throws Exception{
        this.dist_thresh = dist_thresh;
        this.keyRange = keyRange;
        this.LOG = LOG;
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
    }


    @Override
    public void flatMap(Tuple6<Integer, String, Integer, Long, Integer, Double[]> t, Collector<Tuple9<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[]>> collector)
            throws Exception {

        Double[] emb = t.f5;
        int part_num = partitions.size();

        PriorityQueue<Tuple2<Integer, Double>> distances =
                new PriorityQueue<Tuple2<Integer, Double>>(new CustomComparator());

        boolean isOutlier = false;

        if (t.f1.equals("pOuter")) {
            for (Map.Entry<Integer, Tuple3<Long, Integer, Double[]>> centroid : partitions.entrySet()) {
                Double dist = SimilarityJoinsUtil.AngularDistance(emb, centroid.getValue().f2);
                distances.add(new Tuple2<>(centroid.getKey(), dist));

                if (dist <= 2 * dist_thresh) {
                    collector.collect(new Tuple9<>(centroid.getKey(), "outer", t.f0, t.f1, t.f2, -1, t.f3, t.f4, t.f5));
//                    LOG.info(new Tuple9<>(centroid.getKey(), "outer", t.f0, t.f1, t.f2, -1, t.f3, t.f4, t.f5).toString());
                }
            }
            phyOuters.add(t);
        }
        else if (t.f1.equals("pInner")) {
            if (part_num == 0) {
                partitions.put(1, new Tuple3<Long, Integer, Double[]>(t.f3, t.f4, emb));
                collector.collect(new Tuple9<>(1, "inner", t.f0, t.f1, t.f2, 1, t.f3, t.f4, t.f5));
//                LOG.info(new Tuple9<>(1, "inner", t.f0, t.f1, t.f2, 1,t.f3, t.f4, t.f5).toString());
                for(Tuple6<Integer,String,Integer,Long,Integer,Double[]> po : phyOuters.get()){
                    Double[] temp = po.f5;
                    if(SimilarityJoinsUtil.AngularDistance(emb, temp) <= 2 * dist_thresh){
                        collector.collect(new Tuple9<>(1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5));
//                        LOG.info(new Tuple9<>(1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5).toString());
                    }
                }
            } else {
                int inner;
                for (Map.Entry<Integer, Tuple3<Long, Integer, Double[]>> centroid : partitions.entrySet()) {
                    Double dist = SimilarityJoinsUtil.AngularDistance(emb, centroid.getValue().f2);
                    distances.add(new Tuple2<>(centroid.getKey(), dist));

                    if (dist <= 0.5 * dist_thresh) {
                        collector.collect(new Tuple9<>(centroid.getKey(), "inner", t.f0, t.f1, t.f2, centroid.getKey(), t.f3, t.f4, t.f5));
//                        LOG.info(new Tuple9<>(centroid.getKey(), "inner", t.f0, t.f1, t.f2, centroid.getKey(), t.f3, t.f4, t.f5).toString());
                        inner = centroid.getKey();
                    }
                }
//                    if(t.f4.equals(984) || t.f4.equals(21)){
//                        System.out.println(t.f4);
//                        System.out.println(distances.toString());
//                    }
                try {
                    if (distances.peek().f1 > dist_thresh) {
                        inner = part_num + 1;
                        partitions.put(part_num + 1, new Tuple3<>(t.f3, t.f4, emb));
                        collector.collect(new Tuple9<>(part_num + 1, "inner", t.f0, t.f1, t.f2, part_num + 1, t.f3, t.f4, t.f5));
//                        LOG.info(new Tuple9<>(part_num + 1, "inner", t.f0, t.f1, t.f2, part_num + 1, t.f3, t.f4, t.f5).toString());
                        for(Tuple6<Integer,String,Integer,Long,Integer,Double[]> po : phyOuters.get()){
                            Double[] temp = po.f5;

                            if(SimilarityJoinsUtil.AngularDistance(emb, temp) <= 2 * dist_thresh){
                                collector.collect(new Tuple9<>(part_num + 1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5));
//                                LOG.info(new Tuple9<>(part_num + 1, "outer", po.f0, po.f1, po.f2, -1, po.f3, po.f4, po.f5).toString());
                            }
                        }
                        for (Tuple7<Integer, Integer, String, Integer, Long, Integer, Double[]> out : outliers.get()) {
                            Double[] temp = out.f6;
//                            if(out.f4 == 21){
//                                System.out.println(1);
//                                System.out.println(SimilarityJoinsUtil.AngularDistance(emb, temp));
//                            }
//                            if (SimilarityJoinsUtil.AngularDistance(emb, temp) < 1.5 * dist_thresh) {
//                                if (SimilarityJoinsUtil.AngularDistance(emb, partitions.get(out.f0).f2) > 1 * dist_thresh) {
//                                    collector.collect(new Tuple9<>(part_num + 1, "ind_outer", out.f1, out.f2, out.f3, out.f0, out.f4, out.f5, out.f6));
//                                    LOG.info(new Tuple9<>(part_num + 1, "ind_outer", out.f1, out.f2, out.f3, out.f0, out.f4, out.f5, out.f6).toString());
//                                }
//                            }
                        }
                    } else {
                        if (distances.peek().f1 > 0.5 * dist_thresh) {
                            inner = distances.peek().f0;
                            outliers.add(new Tuple7<>(inner,t.f0, t.f1, t.f2, t.f3, t.f4, t.f5));
                            isOutlier = true;
                            collector.collect(
                                    new Tuple9<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[]>(distances.peek().f0, "outlier", t.f0, t.f1, t.f2, distances.peek().f0, t.f3, t.f4, t.f5));
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
                                collector.collect(new Tuple9<>(temp.f0, "outer", t.f0, t.f1, t.f2, inner, t.f3, t.f4, t.f5));
//                                LOG.info(new Tuple9<>(temp.f0, "outer", t.f0, t.f1, t.f2, inner, t.f3, t.f4, t.f5).toString());
                            }
//                            if (isOutlier && (inner < temp.f0)) {
//                                Double[] tempEmb = partitions.get(temp.f0).f2;
//                                Double[] innerEmb = partitions.get(inner).f2;
//
//                                if (SimilarityJoinsUtil.AngularDistance(tempEmb, innerEmb) > 1 * dist_thresh) {
//                                    collector.collect(new Tuple9<>(temp.f0, "ind_outer", t.f0, t.f1, t.f2, inner, t.f3, t.f4, t.f5));
//                                    LOG.info(new Tuple9<>(temp.f0, "ind_outer", t.f0, t.f1, t.f2, inner, t.f3, t.f4, t.f5).toString());
//                                }
//                            }
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