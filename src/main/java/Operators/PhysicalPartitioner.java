package Operators;

import Utils.SimilarityJoinsUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;



public class PhysicalPartitioner implements FlatMapFunction<Tuple3<Long,Integer,Double[]>, Tuple6<Integer,String,Integer, Long,Integer,Double[]>> {

    Double dist_thresh;
    HashMap<Integer, Double[]> partCentroids;
    int keyRange;

    public PhysicalPartitioner(Double dist_thresh, HashMap<Integer, Double[]> randomCentroids, int keyRange) throws Exception{
        this.dist_thresh = dist_thresh;
        this.partCentroids = randomCentroids;
        this.keyRange = keyRange;
    }

    @Override
    public void flatMap(Tuple3<Long, Integer, Double[]> t, Collector<Tuple6<Integer, String, Integer, Long, Integer, Double[]>> collector) throws Exception {

        int numPartitions = 0;
        for (Map.Entry<Integer, Double[]> e : partCentroids.entrySet()){
            numPartitions++;
        }
//            LOG.info(partCentroids.entrySet().toString());

        Double[] distances = new Double[numPartitions];
        int min_idx = 0;
        double min_dist = 1000000000.0;
        for(Map.Entry<Integer, Double[]> centroid : partCentroids.entrySet()){
            double temp = SimilarityJoinsUtil.AngularDistance(centroid.getValue(), t.f2);
            if (min_dist > temp){
                min_idx = centroid.getKey();
                min_dist = temp;
            }
            distances[centroid.getKey()] = temp;
        }
        collector.collect(new Tuple6<Integer,String,Integer,Long,Integer,Double[]>(computePartitionID(min_idx), "pInner", computePartitionID(min_idx), t.f0, t.f1, t.f2));

        for(int i=0; i<distances.length ; i++) {
//                if(t.f1 == 994 || t.f1 == 830){
//                    System.out.println(t.f1);
//                    System.out.format("part: %d, dist: %f, min_dist: %f, exp: %b\n", i, distances[i], min_dist, (distances[i] < min_dist + 2 * dist_thresh));
//                    System.out.format("2nd exp: %b\n", (min_idx < i) ^ (min_idx + i) % 2 == 1);
//                }
            if (i == min_idx) continue;
            else if ((distances[i] <= min_dist + 2 * dist_thresh) && ((min_idx < i) ^ (min_idx + i) % 2 == 1)) {
                collector.collect(new Tuple6<Integer, String, Integer, Long, Integer, Double[]>(
                        computePartitionID(i), "pOuter", computePartitionID(min_idx), t.f0, t.f1, t.f2
                ));
            }
        }
    }

    int computePartitionID(int groupID){
        return groupID*keyRange;
    }
}

