package Operators;

import CustomDataTypes.InputTuple;
import CustomDataTypes.SPTuple;
import Utils.SimilarityJoinsUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;



public class PhysicalPartitioner implements FlatMapFunction<InputTuple, SPTuple> {

    Double dist_thresh;
    HashMap<Integer, Double[]> partCentroids;
    int keyRange;

    public PhysicalPartitioner(Double dist_thresh, HashMap<Integer, Double[]> randomCentroids, int keyRange) throws Exception{
        this.dist_thresh = dist_thresh;
        this.partCentroids = randomCentroids;
        this.keyRange = keyRange;
    }

    @Override
    public void flatMap(InputTuple t, Collector<SPTuple> collector) throws Exception {

        int numPartitions = 0;
        for (Map.Entry<Integer, Double[]> e : partCentroids.entrySet()){
            numPartitions++;
        }
//            LOG.info(partCentroids.entrySet().toString());

        // For each tuple calculate the distances from all the given centroids.
        // Pick the closest one, and assign the tuple to its inner partition.
        Double[] distances = new Double[numPartitions];
        int min_idx = 0;
        double min_dist = 1000000000.0;
        for(Map.Entry<Integer, Double[]> centroid : partCentroids.entrySet()){
            double temp = SimilarityJoinsUtil.AngularDistance(centroid.getValue(), t.f3);
            if (min_dist > temp){
                min_idx = centroid.getKey();
                min_dist = temp;
            }
            distances[centroid.getKey()] = temp;
        }
        collector.collect(new SPTuple(computePartitionID(min_idx), "pInner", computePartitionID(min_idx), t.f0, t.f1, t.f2, t.f3));

        // Assign tuple to the outer partitions of the centroids that satisfy the distance criterion
        // and the routing criterion.
        for(int i=0; i<distances.length ; i++) {
            if (i == min_idx) continue;
            else if ((distances[i] <= min_dist + 2 * dist_thresh) && ((min_idx < i) ^ (min_idx + i) % 2 == 1)) {
                collector.collect(new SPTuple(
                        computePartitionID(i), "pOuter", computePartitionID(min_idx), t.f0, t.f1, t.f2, t.f3
                ));
            }
        }
    }

    int computePartitionID(int groupID){
        int parallelism = partCentroids.keySet().size();
        return ((groupID * 128 + parallelism - 1) / parallelism);
    }
}

