import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.metrics.util.SystemResourcesCounter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.*;


public class onlinePartitioningForSsj {

    private static final Logger LOG = LoggerFactory.getLogger(onlinePartitioningForSsj.class);

    static String pwd = Paths.get("").toAbsolutePath().toString();

    public static class PhysicalPartitioner implements FlatMapFunction<Tuple3<Long,Integer,String>, Tuple5<Integer,String,Long,Integer,String>>{

        HashMap<String, Double[]> wordEmbeddings;
        Double dist_thresh;
        HashMap<Integer, Double[]> partCentroids;
        int keyRange;

        public PhysicalPartitioner(String file4WE, Double dist_thresh, HashMap<Integer, Double[]> randomCentroids, int keyRange) throws Exception{
            this.wordEmbeddings = SimilarityJoinsUtil.readEmbeddings(file4WE);
            this.dist_thresh = dist_thresh;
            this.partCentroids = randomCentroids;
            this.keyRange = keyRange;
        }

        @Override
        public void flatMap(Tuple3<Long, Integer, String> t, Collector<Tuple5<Integer, String, Long, Integer, String>> collector) throws Exception {

            int numPartitions = 0;
            for (Map.Entry<Integer, Double[]> e : partCentroids.entrySet()){
                numPartitions++;
            }
//            LOG.info(partCentroids.entrySet().toString());

            Double[] distances = new Double[numPartitions];
            int min_idx = 0;
            double min_dist = 1000000000.0;
            for(Map.Entry<Integer, Double[]> centroid : partCentroids.entrySet()){
                double temp = SimilarityJoinsUtil.CosineDistance(centroid.getValue(), wordEmbeddings.get(t.f2));
                if (min_dist > temp){
                    min_idx = centroid.getKey();
                    min_dist = temp;
                }
                distances[centroid.getKey()] = temp;
            }
            collector.collect(new Tuple5<Integer,String,Long,Integer,String>(computePartitionID(min_idx), "pInner", t.f0, t.f1, t.f2));

            for(int i=0; i<distances.length ; i++){
                if(i == min_idx) continue;
                else if((distances[i] < min_dist + 2*dist_thresh) && ((min_idx < i) ^ (min_idx + i)%2 == 1)){
                    collector.collect(new Tuple5<Integer,String,Long,Integer,String>(
                            computePartitionID(i), "pOuter", t.f0, t.f1, t.f2
                    ));
                }
//                if(t.f1.equals(785) || t.f1.equals(4)){
//                    System.out.println(t.f1.toString());
//                    System.out.println((distances[i] < min_dist + 2*dist_thresh) && ((min_idx < i) ^ (min_idx + i)%2 == 1));
//                }
            }
//            if(t.f1.equals(785) || t.f1.equals(4)){
//                String temp = "";
//                for (Double d : distances){
//                    temp = temp + d.toString() + ",";
//                }
//                System.out.println(t.f1.toString());
//                System.out.println(temp);
//            }

        }

        int computePartitionID(int groupID){
            return groupID*keyRange;
        }
    }

    public static class AdaptivePartitioner extends
            RichFlatMapFunction<Tuple5<Integer,String,Long,Integer,String>, Tuple7<Integer,String,Integer,String,Long,Integer,String>> {

        HashMap<String, Double[]> wordEmbeddings;
        Double dist_thresh;
        int keyRange;
        HashMap<Integer, Tuple3<Long, String, Double[]>> partitions = new HashMap<>();
        ListState<Tuple6<Integer,Integer,String,Long,Integer,String>> outliers;
        ListState<Tuple5<Integer,String,Long,Integer,String>> phyOuters;

        public AdaptivePartitioner(String file4WE, Double dist_thresh, int keyRange) throws Exception{
            this.wordEmbeddings = SimilarityJoinsUtil.readEmbeddings(file4WE);
            this.dist_thresh = dist_thresh;
            this.keyRange = keyRange;
        }

        @Override
        public void open(Configuration config){
            ListStateDescriptor<Tuple6<Integer,Integer,String,Long,Integer,String>> outliersDesc =
                    new ListStateDescriptor<Tuple6<Integer, Integer, String, Long, Integer, String>>(
                            "outliers",
                            TypeInformation.of(new TypeHint<Tuple6<Integer, Integer, String, Long, Integer, String>>() {})
                            );
            outliers = getRuntimeContext().getListState(outliersDesc);

            ListStateDescriptor<Tuple5<Integer,String,Long,Integer,String>> phyOutersDesc =
                    new ListStateDescriptor<Tuple5<Integer, String, Long, Integer, String>>(
                            "phyOuters",
                            TypeInformation.of(new TypeHint<Tuple5<Integer, String, Long, Integer, String>>() {})
                    );
            phyOuters = getRuntimeContext().getListState(phyOutersDesc);
        }


        @Override
        public void flatMap(Tuple5<Integer, String, Long, Integer, String> t, Collector<Tuple7<Integer, String, Integer, String, Long, Integer, String>> collector)
                throws Exception {

            Double[] emb = wordEmbeddings.get(t.f4);
            int part_num = partitions.size();

            PriorityQueue<Tuple2<Integer, Double>> distances =
                    new PriorityQueue<Tuple2<Integer, Double>>(new CustomComparator());

            boolean isOutlier = false;

            if (t.f1.equals("pOuter")) {
                for (Map.Entry<Integer, Tuple3<Long, String, Double[]>> centroid : partitions.entrySet()) {
                    Double dist = SimilarityJoinsUtil.CosineDistance(emb, centroid.getValue().f2);
                    distances.add(new Tuple2<>(centroid.getKey(), dist));

                    if (dist <= 2 * dist_thresh) {
                        collector.collect(new Tuple7<>(centroid.getKey(), "outer", t.f0, t.f1, t.f2, t.f3, t.f4));
                        LOG.info(new Tuple7<>(centroid.getKey(), "outer", t.f0, t.f1, t.f2, t.f3, t.f4).toString());
                    }
                }
                phyOuters.add(t);
                if(t.f3.equals(556) && t.f0.equals(39)){
                    LOG.info(distances.toString());
                }
            }
            else if (t.f1.equals("pInner")) {
                if (part_num == 0) {
                    partitions.put(1, new Tuple3<Long, String, Double[]>(t.f2, t.f4, emb));
                    collector.collect(new Tuple7<>(1, "inner", t.f0, t.f1, t.f2, t.f3, t.f4));
                    LOG.info(new Tuple7<>(1, "inner", t.f0, t.f1, t.f2, t.f3, t.f4).toString());
                    for(Tuple5<Integer,String,Long,Integer,String> po : phyOuters.get()){
                        Double[] temp = wordEmbeddings.get(po.f4);
                        if(SimilarityJoinsUtil.CosineDistance(emb, temp) <= 2 * dist_thresh){
                            collector.collect(new Tuple7<>(1, "outer", po.f0, po.f1, po.f2, po.f3, po.f4));
                            LOG.info(new Tuple7<>(1, "outer", po.f0, po.f1, po.f2, po.f3, po.f4).toString());
                        }
                    }
                } else {
                    int inner;
                    for (Map.Entry<Integer, Tuple3<Long, String, Double[]>> centroid : partitions.entrySet()) {
                        Double dist = SimilarityJoinsUtil.CosineDistance(emb, centroid.getValue().f2);
                        distances.add(new Tuple2<>(centroid.getKey(), dist));

                        if (dist <= 0.5 * dist_thresh) {
                            collector.collect(new Tuple7<>(centroid.getKey(), "inner", t.f0, t.f1, t.f2, t.f3, t.f4));
                            LOG.info(new Tuple7<>(centroid.getKey(), "inner", t.f0, t.f1, t.f2, t.f3, t.f4).toString());
                            inner = centroid.getKey();
                        }
                    }
//                    if(t.f3.equals(993)){
//                        System.out.println(distances.toString());
//                    }
                    try {
                        if (distances.peek().f1 > dist_thresh) {
                            inner = part_num + 1;
                            partitions.put(part_num + 1, new Tuple3<>(t.f2, t.f4, emb));
                            collector.collect(new Tuple7<>(part_num + 1, "inner", t.f0, t.f1, t.f2, t.f3, t.f4));
                            LOG.info(new Tuple7<>(part_num + 1, "inner", t.f0, t.f1, t.f2, t.f3, t.f4).toString());
                            for(Tuple5<Integer,String,Long,Integer,String> po : phyOuters.get()){
                                Double[] temp = wordEmbeddings.get(po.f4);
                                if(SimilarityJoinsUtil.CosineDistance(emb, temp) <= 2 * dist_thresh){
                                    collector.collect(new Tuple7<>(part_num + 1, "outer", po.f0, po.f1, po.f2, po.f3, po.f4));
                                    LOG.info(new Tuple7<>(part_num + 1, "outer", po.f0, po.f1, po.f2, po.f3, po.f4).toString());
                                }
                            }
                            for (Tuple6<Integer, Integer, String, Long, Integer, String> out : outliers.get()) {
                                Double[] temp = wordEmbeddings.get(out.f5);
                                if (SimilarityJoinsUtil.CosineDistance(emb, temp) < 1.5 * dist_thresh) {
                                    if (SimilarityJoinsUtil.CosineDistance(emb, partitions.get(out.f0).f2) > 1.5 * dist_thresh) {
                                        collector.collect(new Tuple7<>(part_num + 1, "ind_outer", out.f1, out.f2, out.f3, out.f4, out.f5));
                                        LOG.info(new Tuple7<>(part_num + 1, "ind_outer", out.f1, out.f2, out.f3, out.f4, out.f5).toString());
                                    }
                                }
                            }
                        } else {
                            if (distances.peek().f1 > 0.5 * dist_thresh) {
                                inner = distances.peek().f0;
                                outliers.add(new Tuple6<>(inner, t.f0, t.f1, t.f2, t.f3, t.f4));
                                isOutlier = true;
                                collector.collect(
                                        new Tuple7<Integer, String, Integer, String, Long, Integer, String>(distances.peek().f0, "outlier", t.f0, t.f1, t.f2, t.f3, t.f4));
                                LOG.info(new Tuple7<Integer, String, Integer, String, Long, Integer, String>(distances.peek().f0, "outlier", t.f0, t.f1, t.f2, t.f3, t.f4).toString());
                            } else {
                                inner = distances.peek().f0;
                            }
                        }

                        while (!distances.isEmpty()) {
                            Tuple2<Integer, Double> temp = distances.poll();
                            if (temp.f0 == inner) {
                                continue;
                            } else if (temp.f1 > 1.5 * dist_thresh) {
                                break;
                            } else {
                                if (inner > temp.f0) {
                                    collector.collect(new Tuple7<>(temp.f0, "outer", t.f0, t.f1, t.f2, t.f3, t.f4));
                                    LOG.info(new Tuple7<>(temp.f0, "outer", t.f0, t.f1, t.f2, t.f3, t.f4).toString());
                                }
                                if (isOutlier && (inner < temp.f0)) {
                                    Double[] tempEmb = partitions.get(temp.f0).f2;
                                    Double[] innerEmb = partitions.get(inner).f2;

                                    if (SimilarityJoinsUtil.CosineDistance(tempEmb, innerEmb) > 1.5 * dist_thresh) {
                                        collector.collect(new Tuple7<>(temp.f0, "ind_outer", t.f0, t.f1, t.f2, t.f3, t.f4));
                                        LOG.info(new Tuple7<>(temp.f0, "ind_outer", t.f0, t.f1, t.f2, t.f3, t.f4).toString());
                                    }

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

    public static class CustomOnElementTrigger extends Trigger<Tuple7<Integer,String,Integer,String,Long,Integer,String>, GlobalWindow>{

        @Override
        public TriggerResult onElement(Tuple7<Integer,String,Integer,String,Long,Integer,String> t, long l, GlobalWindow window, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, GlobalWindow window, TriggerContext triggerContext) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onEventTime(long l, GlobalWindow window, TriggerContext triggerContext) throws Exception {
            return null;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext triggerContext) throws Exception {

        }
    }

    public static class SimilarityJoin extends ProcessWindowFunction<Tuple7<Integer,String,Integer,String,Long,Integer,String>,
                    Tuple3<Boolean, Tuple7<Integer,String,Integer,String,Long,Integer,String>,Tuple7<Integer,String,Integer,String,Long,Integer,String>>,
                    Tuple2<Integer,Integer>,
                    GlobalWindow> {

        HashMap<String, Double[]> wordEmbeddings;
        Double dist_thresh;

        public SimilarityJoin(String file4WE, Double dist_thresh)throws Exception{
            this.wordEmbeddings = SimilarityJoinsUtil.readEmbeddings(file4WE);
            this.dist_thresh = dist_thresh;
        }


        @Override
        public void process(Tuple2<Integer,Integer> key,
                          Context ctx,
                          Iterable<Tuple7<Integer, String, Integer, String, Long, Integer, String>> tuples,
                          Collector<Tuple3<Boolean,
                                  Tuple7<Integer, String, Integer, String, Long, Integer, String>,
                                  Tuple7<Integer, String, Integer, String, Long, Integer, String>>> collector)
                throws Exception {

            Iterator<Tuple7<Integer, String, Integer, String, Long, Integer, String>> tuplesIterator = tuples.iterator();
            LinkedList<Tuple7<Integer, String, Integer, String, Long, Integer, String>> tuplesList = new LinkedList<>();
            tuplesIterator.forEachRemaining(tuplesList::addFirst);

            Tuple7<Integer, String, Integer, String, Long, Integer, String> newTuple = tuplesList.pollFirst();
            Double[] newTupleEmbed = wordEmbeddings.get(newTuple.f6);

            for (Tuple7<Integer, String, Integer, String, Long, Integer, String> t : tuplesList ) {

//                LOG.info(newTuple.toString()+", "+t.toString());

                boolean exp = (
                                (newTuple.f1.equals("outer") && t.f1.equals("inner")) ||
                                (newTuple.f1.equals("outlier") && t.f1.equals("outlier")) ||
                                        (newTuple.f1.equals("outlier") && t.f1.equals("inner")) ||
                                        (newTuple.f1.equals("inner") && t.f1.equals("outlier")) ||
                                (newTuple.f1.equals("inner") && t.f1.equals("outer")) ||
                                        (newTuple.f1.equals("outlier") && t.f1.equals("outer") && !t.f5.equals(newTuple.f5)) ||
                                        (newTuple.f1.equals("outer") && t.f1.equals("outlier") && !t.f5.equals(newTuple.f5)) ||
                                        (newTuple.f1.equals("ind_outer") && t.f1.equals("inner")) ||
                                        (newTuple.f1.equals("inner") && t.f1.equals("ind_outer"))
                        );

                if (exp){
                    Double[] tEmbed = wordEmbeddings.get(t.f6);
                    if(newTuple.f5 > t.f5) {
                        collector.collect(
                                new Tuple3<>(
                                        (SimilarityJoinsUtil.CosineDistance(newTupleEmbed, tEmbed) < dist_thresh),
                                        newTuple,
                                        t
                                )
                        );
                    }
                    else{
                        collector.collect(
                                new Tuple3<>(
                                        (SimilarityJoinsUtil.CosineDistance(newTupleEmbed, tEmbed) < dist_thresh),
                                        t,
                                        newTuple
                                )
                        );
                    }
                }
                else if(newTuple.f1.equals("inner") && t.f1.equals("inner")){
                    if(newTuple.f5 > t.f5) {
                        collector.collect(
                                new Tuple3<>(
                                        true,
                                        newTuple,
                                        t
                                )
                        );
                    }
                    else{
                        collector.collect(
                                new Tuple3<>(
                                        true,
                                        t,
                                        newTuple
                                )
                        );
                    }
                }
            }
        }
    }

    static public class CustomFiltering extends ProcessFunction<Tuple3<Boolean, Tuple7<Integer,String,Integer,String,Long,Integer,String>, Tuple7<Integer,String,Integer,String,Long,Integer,String>>,Tuple3<Boolean, Tuple7<Integer,String,Integer,String,Long,Integer,String>, Tuple7<Integer,String,Integer,String,Long,Integer,String>>> {

        OutputTag<Tuple3<Boolean, Tuple7<Integer,String,Integer,String,Long,Integer,String>, Tuple7<Integer,String,Integer,String,Long,Integer,String>>> sideStats;

        public CustomFiltering(
                OutputTag<Tuple3<Boolean, Tuple7<Integer,String,Integer,String,Long,Integer,String>, Tuple7<Integer,String,Integer,String,Long,Integer,String>>> sideStats){
            this.sideStats = sideStats;
        }

        @Override
        public void processElement(Tuple3<Boolean, Tuple7<Integer,String,Integer,String,Long,Integer,String>, Tuple7<Integer,String,Integer,String,Long,Integer,String>> t,
                                   Context context, Collector<Tuple3<Boolean,
                Tuple7<Integer,String,Integer,String,Long,Integer,String>, Tuple7<Integer,String,Integer,String,Long,Integer,String>>> collector)
                throws Exception {

            if(t.f0){
                collector.collect(t);
            }
            context.output(sideStats, t);
        }
    }

    public static class LPMeasurePerformance implements MapFunction<Tuple4<Integer, Integer, Boolean, Long>, List<Tuple4<String, String, String, String>>> {

        private HashMap<Tuple2<Integer, Integer>, HashMap<String, Long>> stats = new HashMap<>();

        @Override
        public List<Tuple4<String, String, String, String>> map(Tuple4<Integer, Integer, Boolean, Long> t) throws Exception {
            if(!stats.containsKey(new Tuple2<>(t.f0, t.f1))){
                HashMap<String, Long> temp = new HashMap<>();
                temp.put("false", 0L);
                temp.put("true", 0L);
                stats.put(new Tuple2<>(t.f0, t.f1), temp);
            }
            HashMap<String, Long> upd = stats.get(new Tuple2<>(t.f0, t.f1));
            upd.put(t.f2.toString(), t.f3);
            stats.put(new Tuple2<>(t.f0, t.f1), upd);

            List<Tuple4<String, String, String, String>> out = new ArrayList<>();
            for(Tuple2<Integer,Integer> i : stats.keySet()){
                    out.add(new Tuple4<String, String, String, String>(
                            Integer.toString(i.f0),
                            Integer.toString(i.f1),
                            "false = "+ stats.get(i).get("false").toString(),
                            "true = "+ stats.get(i).get("true").toString()));
            }
            return out;
        }
    }

    public static class PPMeasurePerformance implements MapFunction<Tuple3<Integer, Boolean, Long>, List<Tuple3<String, String, String>>> {

        private HashMap<Integer, HashMap<String, Long>> stats = new HashMap<>();

        @Override
        public List<Tuple3<String, String, String>> map(Tuple3<Integer, Boolean, Long> t) throws Exception {
            if(!stats.containsKey(t.f0)){
                HashMap<String, Long> temp = new HashMap<>();
                temp.put("false", 0L);
                temp.put("true", 0L);
                stats.put(t.f0, temp);
            }
            HashMap<String, Long> upd = stats.get(t.f0);
            upd.put(t.f1.toString(), t.f2);
            stats.put(t.f0, upd);

            List<Tuple3<String, String, String>> out = new ArrayList<>();
            for(Integer i : stats.keySet()){
                out.add(new Tuple3<String, String, String>(
                        Integer.toString(i),
                        "false = "+ stats.get(i).get("false").toString(),
                        "true = "+ stats.get(i).get("true").toString()));
            }
            return out;
        }
    }


    public static class StatsKeySelector implements KeySelector<Tuple4<Integer, Integer, Boolean, Long>, Tuple3<Integer, Integer, Boolean>> {

        @Override
        public Tuple3<Integer, Integer, Boolean> getKey(Tuple4<Integer, Integer, Boolean, Long> t) throws Exception {
            return new Tuple3<Integer, Integer, Boolean>(t.f0, t.f1, t.f2);
        }
    }

    public static class PhyStatsKeySelector implements KeySelector<Tuple3<Integer, Boolean, Long>, Tuple2<Integer, Boolean>> {

        @Override
        public Tuple2<Integer, Boolean> getKey(Tuple3<Integer, Boolean, Long> t) throws Exception {
            return new Tuple2<Integer, Boolean>(t.f0, t.f1);
        }
    }

    public static class LogicalKeySelector implements KeySelector<Tuple7<Integer,String,Integer, String, Long, Integer, String>, Tuple2<Integer, Integer>>{

        @Override
        public Tuple2<Integer, Integer> getKey(Tuple7<Integer, String, Integer, String, Long, Integer, String> t) throws Exception {
            return new Tuple2<Integer, Integer>(t.f0,t.f2);
        }
    }


    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamFactory streamFactory = new StreamFactory(env);
        env.setMaxParallelism(128);
        env.setParallelism(10);

        LOG.info("Enter main.");

        DataStream<Tuple3<Long, Integer, String>> data = streamFactory.createSimpleWordsStream("wordStream.txt");

        DataStream<Tuple5<Integer,String,Long,Integer,String>> ppData = data.
                flatMap(new PhysicalPartitioner("wiki-news-300d-1K.vec", 0.3, SimilarityJoinsUtil.RandomCentroids(10), (env.getMaxParallelism()/env.getParallelism())+1));

//        ppData.print();

        DataStream<Tuple7<Integer,String,Integer,String,Long,Integer,String>> lpData = ppData
                .keyBy(t -> t.f0)
                .flatMap(new AdaptivePartitioner("wiki-news-300d-1K.vec", 0.3, (env.getMaxParallelism()/env.getParallelism())+1));

        //lpData.print();
//        partitionedData.keyBy(t -> t.f0).print();

        final OutputTag<Tuple3<Boolean, Tuple7<Integer,String,Integer,String,Long,Integer,String>, Tuple7<Integer,String,Integer,String,Long,Integer,String>>> sideStats =
                new OutputTag<Tuple3<Boolean, Tuple7<Integer,String,Integer,String,Long,Integer,String>, Tuple7<Integer,String,Integer,String,Long,Integer,String>>>("stats"){};

        SingleOutputStreamOperator<Tuple3<Boolean, Tuple7<Integer,String,Integer,String,Long,Integer,String>,Tuple7<Integer,String,Integer,String,Long,Integer,String>>>
                selfJoinedStream = lpData
                .keyBy(new LogicalKeySelector())
                .window(GlobalWindows.create())
                .trigger(new CustomOnElementTrigger())
                .process(new SimilarityJoin("wiki-news-300d-1K.vec", 0.3))
                .process(new CustomFiltering(sideStats));

        selfJoinedStream.print();

        env.setParallelism(1);
        DataStream<List<Tuple4<String,String,String,String>>> logicalStatistics = selfJoinedStream.getSideOutput(sideStats)
                .map(t -> new Tuple4<>(t.f1.f2, t.f1.f0, t.f0, 1L))
                .returns(TypeInformation.of(new TypeHint<Tuple4<Integer, Integer, Boolean, Long>>() {
                }))
                .keyBy(new StatsKeySelector())
                .sum(3)
                .map(new LPMeasurePerformance());

        DataStream<List<Tuple3<String,String,String>>> physicalStatistics = selfJoinedStream.getSideOutput(sideStats)
                .map(t -> new Tuple3<>(t.f1.f2, t.f0, 1L))
                .returns(TypeInformation.of(new TypeHint<Tuple3<Integer, Boolean, Long>>() {
                }))
                .keyBy(new PhyStatsKeySelector())
                .sum(2)
                .map(new PPMeasurePerformance());

        physicalStatistics.writeAsText(pwd+"/src/main/outputs/stats.txt", FileSystem.WriteMode.OVERWRITE);

        LOG.info(env.getExecutionPlan());

        env.execute();

    }
}

// TODO:
//  - Use embeddings from end-to-end.
//  - Add more metrics
//  - Create Tests.  DONE
//  - Workaround keyBy to control how data are partitioned.     DONE
