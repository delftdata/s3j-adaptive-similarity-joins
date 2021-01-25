import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;


public class onlinePartitioningForSsj {

    private static final Logger LOG = LoggerFactory.getLogger(onlinePartitioningForSsj.class);

    static String pwd = Paths.get("").toAbsolutePath().toString();

    public static class AdaptivePartitioner implements
            FlatMapFunction<Tuple3<Long,Integer,String>, Tuple5<Integer,String,Long,Integer,String>> {

        HashMap<String, Double[]> wordEmbeddings;
        Double dist_thresh;
        HashMap<Integer, Tuple3<Long, String, Double[]>> partitions = new HashMap<>();

        public AdaptivePartitioner(String file4WE, Double dist_thresh) throws Exception{
            this.wordEmbeddings = SimilarityJoinsUtil.readEmbeddings(file4WE);
            this.dist_thresh = dist_thresh;
        }

        @Override
        public void flatMap(Tuple3<Long, Integer, String> t, Collector<Tuple5<Integer, String, Long, Integer, String>> collector)
                throws Exception {

            Double[] emb = wordEmbeddings.get(t.f2);
            int part_num = partitions.size();

            PriorityQueue<Tuple2<Integer, Double>> distances =
                    new PriorityQueue<Tuple2<Integer, Double>>(new CustomComparator());

            boolean centroid_flag = false;


            if (part_num == 0){
                partitions.put(1, new Tuple3<>(t.f0, t.f2, emb));
                collector.collect(new Tuple5<>(1, "inner", t.f0, t.f1, t.f2));
            }
            else{
                for(Map.Entry<Integer, Tuple3<Long, String, Double[]>> centroid : partitions.entrySet()){
                    Double dist = SimilarityJoinsUtil.CosineDistance(emb, centroid.getValue().f2);
                    distances.add(new Tuple2<>(centroid.getKey(),dist));
                    if (dist < dist_thresh){
                        collector.collect(new Tuple5<>(centroid.getKey(), "inner", t.f0, t.f1, t.f2));

                    }
                    else if (dist < 2*dist_thresh){
                        collector.collect(new Tuple5<>(centroid.getKey(), "outer", t.f0, t.f1, t.f2));
                    }
                }
                try {
                    if (distances.peek().f1 > 2*dist_thresh){
                        partitions.put(part_num + 1, new Tuple3<>(t.f0, t.f2, emb));
                        collector.collect(new Tuple5<>(part_num + 1, "inner", t.f0, t.f1, t.f2));
                    }
                    else {
                        if (distances.peek().f1 > dist_thresh) {
                            partitions.put(part_num + 1, new Tuple3<>(t.f0, t.f2, emb));
                            collector.collect(
                                    new Tuple5<Integer, String, Long, Integer, String>(distances.peek().f0, "outlier", t.f0, t.f1, t.f2));
                        }
                    }
                }
                catch (Exception e){
                    System.out.println(e.getMessage());
                    throw e;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamFactory streamFactory = new StreamFactory(env);
        env.setParallelism(1);

        LOG.info("Enter main.");

        DataStream<Tuple3<Long, Integer, String>> data = streamFactory.createSimpleWordsStream();

        DataStream<Tuple5<Integer,String,Long,Integer,String>> partitionedData = data
                .flatMap(new AdaptivePartitioner("wiki-news-300d-1K.vec", 0.3))
                .keyBy(t -> t.f0);
        partitionedData.print();

        LOG.info(env.getExecutionPlan());

        env.execute();

    }
}
