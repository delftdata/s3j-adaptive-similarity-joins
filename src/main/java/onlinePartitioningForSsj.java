import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
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

    public static class AdaptivePartitioner implements
            FlatMapFunction<Tuple3<Long,Integer,String>, Tuple5<Integer,String,Long,Integer,String>> {

        HashMap<String, Double[]> wordEmbeddings;
        Double dist_thresh;
        int keyRange;
        HashMap<Integer, Tuple3<Long, String, Double[]>> partitions = new HashMap<>();
        HashMap<Integer, Tuple2<Integer, Integer>> partGroups = new HashMap<>();
        int nextGroup = 1;

        public AdaptivePartitioner(String file4WE, Double dist_thresh, int keyRange) throws Exception{
            this.wordEmbeddings = SimilarityJoinsUtil.readEmbeddings(file4WE);
            this.dist_thresh = dist_thresh;
            this.keyRange = keyRange;
            partGroups.put(0, new Tuple2<>(0, 0));
        }

        int computePartitionID(int hugeGroup, int groupID, int nextPartIDinGroup){
            System.out.println(keyRange);
            return hugeGroup*128 + groupID*keyRange + nextPartIDinGroup;
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
//                    System.out.format("centroid: %d, new: %d, dist: %f\n", centroid.getKey(), t.f1,dist);
                    distances.add(new Tuple2<>(centroid.getKey(),dist));
                    if (dist <= 0.5*dist_thresh){
                        collector.collect(new Tuple5<>(centroid.getKey(), "inner", t.f0, t.f1, t.f2));

                    }
                    else if (dist < 1.5*dist_thresh){
                        collector.collect(new Tuple5<>(centroid.getKey(), "outer", t.f0, t.f1, t.f2));
                    }
                }
                try {
                    if (distances.peek().f1 > dist_thresh){
                        part_num++;
                        if(distances.peek().f1 > 1.5*dist_thresh) {
                            int groupID = nextGroup++;
                            int nextPartIDinGroup = 0;
                            int hugeGroup = 0;
                            int partID = computePartitionID(hugeGroup, groupID, nextPartIDinGroup);
                            System.out.format("%d, %d\n", t.f1 ,partID);
                            partitions.put(partID, new Tuple3<>(t.f0, t.f2, emb));
                            collector.collect(new Tuple5<>(part_num + 1, "inner", t.f0, t.f1, t.f2));
                            partGroups.put(groupID, new Tuple2<>(nextPartIDinGroup,hugeGroup));
                        }
                        else{
                            int groupID = (distances.peek().f0%128)/keyRange;
                            int nextPartIDinGroup = 0;
                            if (partGroups.get(groupID).f0<12){
                                nextPartIDinGroup = partGroups.get(groupID).f0+1;
                                partGroups.put(groupID,new Tuple2<>(nextPartIDinGroup, partGroups.get(groupID).f1));
                            }
                            else{
                                partGroups.put(groupID,new Tuple2<>(nextPartIDinGroup, partGroups.get(groupID).f1+1));
                            }
                            int hugeGroup = partGroups.get(groupID).f1;
                            int partID = computePartitionID(hugeGroup, groupID, nextPartIDinGroup);
                            System.out.format("%d, %d\n", t.f1 ,partID);
                            partitions.put(partID, new Tuple3<>(t.f0, t.f2, emb));
                            collector.collect(new Tuple5<>(partID, "inner", t.f0, t.f1, t.f2));
                        }
                    }
                    else {
                        if (distances.peek().f1 > 0.5*dist_thresh) {
                            collector.collect(
                                    new Tuple5<Integer, String, Long, Integer, String>(distances.peek().f0, "outlier", t.f0, t.f1, t.f2));
                        }
                    }
                }
                catch (Exception e){
                    System.out.println("oups:" + e.getMessage() +" on "+ (distances.peek().f0%128)/keyRange);
                    throw e;
                }
            }
        }
    }

    public static class CustomOnElementTrigger extends Trigger<Tuple5<Integer,String,Long,Integer,String>, GlobalWindow>{

        @Override
        public TriggerResult onElement(Tuple5<Integer,String,Long,Integer,String> t, long l, GlobalWindow window, TriggerContext triggerContext) throws Exception {
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

    public static class SimilarityJoin extends ProcessWindowFunction<Tuple5<Integer,String,Long,Integer,String>,
                    Tuple3<Boolean, Tuple5<Integer,String,Long,Integer,String>,Tuple5<Integer,String,Long,Integer,String>>,
                    Integer,
                    GlobalWindow> {

        HashMap<String, Double[]> wordEmbeddings;
        Double dist_thresh;

        public SimilarityJoin(String file4WE, Double dist_thresh)throws Exception{
            this.wordEmbeddings = SimilarityJoinsUtil.readEmbeddings(file4WE);
            this.dist_thresh = dist_thresh;
        }


        @Override
        public void process(Integer integer,
                          Context ctx,
                          Iterable<Tuple5<Integer, String, Long, Integer, String>> tuples,
                          Collector<Tuple3<Boolean,
                                  Tuple5<Integer, String, Long, Integer, String>,
                                  Tuple5<Integer, String, Long, Integer, String>>> collector)
                throws Exception {

            Iterator<Tuple5<Integer, String, Long, Integer, String>> tuplesIterator = tuples.iterator();
            LinkedList<Tuple5<Integer, String, Long, Integer, String>> tuplesList = new LinkedList<>();
            tuplesIterator.forEachRemaining(tuplesList::addFirst);

            Tuple5<Integer, String, Long, Integer, String> newTuple = tuplesList.pollFirst();
            Double[] newTupleEmbed = wordEmbeddings.get(newTuple.f4);

            for (Tuple5<Integer, String, Long, Integer, String> t : tuplesList ) {

                boolean exp = (
                                (newTuple.f1.equals("outer") && t.f1.equals("inner")) ||
                                (newTuple.f1.equals("outlier") && t.f1.equals("outlier")) ||
                                (newTuple.f1.equals("inner") && t.f1.equals("outer"))
                        );

                if (exp){
                    Double[] tEmbed = wordEmbeddings.get(t.f4);
                    collector.collect(
                            new Tuple3<>(
                                    (SimilarityJoinsUtil.CosineDistance(newTupleEmbed, tEmbed) < dist_thresh),
                                    newTuple,
                                    t
                            )
                        );
                }
                else if(newTuple.f1.equals("inner") && t.f1.equals("inner")){
                    collector.collect(
                            new Tuple3<>(
                                    true,
                                    newTuple,
                                    t
                            )
                    );
                }
            }
        }
    }

    static public class CustomFiltering extends ProcessFunction<Tuple3<Boolean, Tuple5<Integer,String,Long,Integer,String>, Tuple5<Integer,String,Long,Integer,String>>,Tuple3<Boolean, Tuple5<Integer,String,Long,Integer,String>, Tuple5<Integer,String,Long,Integer,String>>> {

        OutputTag<Tuple3<Boolean, Tuple5<Integer,String,Long,Integer,String>, Tuple5<Integer,String,Long,Integer,String>>> sideStats;

        public CustomFiltering(
                OutputTag<Tuple3<Boolean, Tuple5<Integer,String,Long,Integer,String>, Tuple5<Integer,String,Long,Integer,String>>> sideStats){
            this.sideStats = sideStats;
        }

        @Override
        public void processElement(Tuple3<Boolean, Tuple5<Integer,String,Long,Integer,String>, Tuple5<Integer,String,Long,Integer,String>> t,
                                   Context context, Collector<Tuple3<Boolean,
                Tuple5<Integer,String,Long,Integer,String>, Tuple5<Integer,String,Long,Integer,String>>> collector)
                throws Exception {

            if(t.f0){
                collector.collect(t);
            }
            context.output(sideStats, t);
        }
    }

    public static class MeasurePerformance implements MapFunction<Tuple3<Integer, Boolean, Long>, List<Tuple3<String, String, String>>> {

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
            for(int i : stats.keySet()){
                    out.add(new Tuple3<String, String, String>(
                            Integer.toString(i),
                            "false = "+ stats.get(i).get("false").toString(),
                            "true = "+ stats.get(i).get("true").toString()));
            }
            return out;
        }
    }


    public static class Tuple2KeySelector implements KeySelector<Tuple3<Integer, Boolean, Long>, Tuple2<Integer, Boolean>> {

        @Override
        public Tuple2<Integer, Boolean> getKey(Tuple3<Integer, Boolean, Long> t) throws Exception {
            return new Tuple2<Integer, Boolean>(t.f0, t.f1);
        }
    }


    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamFactory streamFactory = new StreamFactory(env);
        env.setMaxParallelism(128);
        env.setParallelism(1);

        LOG.info("Enter main.");

        System.out.println((env.getMaxParallelism()));

        DataStream<Tuple3<Long, Integer, String>> data = streamFactory.createSimpleWordsStream();

        DataStream<Tuple5<Integer,String,Long,Integer,String>> partitionedData = data
                .flatMap(new AdaptivePartitioner("wiki-news-300d-1K.vec", 0.3, (env.getMaxParallelism()/env.getParallelism()))).setParallelism(1);

//        partitionedData.keyBy(t -> t.f0).print();

        final OutputTag<Tuple3<Boolean, Tuple5<Integer,String,Long,Integer,String>, Tuple5<Integer,String,Long,Integer,String>>> sideStats =
                new OutputTag<Tuple3<Boolean, Tuple5<Integer,String,Long,Integer,String>, Tuple5<Integer,String,Long,Integer,String>>>("stats"){};

        SingleOutputStreamOperator<Tuple3<Boolean, Tuple5<Integer,String,Long,Integer,String>,Tuple5<Integer,String,Long,Integer,String>>>
                selfJoinedStream = partitionedData
                .keyBy(t-> t.f0)
                .window(GlobalWindows.create())
                .trigger(new CustomOnElementTrigger())
                .process(new SimilarityJoin("wiki-news-300d-1K.vec", 0.3))
                .process(new CustomFiltering(sideStats));

        selfJoinedStream.print();

        env.setParallelism(1);
        DataStream<List<Tuple3<String,String,String>>> statistics = selfJoinedStream.getSideOutput(sideStats)
                .map(t -> new Tuple3<>(t.f1.f0 ,t.f0, 1L))
                .returns(TypeInformation.of(new TypeHint<Tuple3<Integer ,Boolean, Long>>() {
                }))
                .keyBy(new Tuple2KeySelector())
                .sum(2)
                .map(new MeasurePerformance());

        statistics.writeAsText(pwd+"/src/main/outputs/stats.txt", FileSystem.WriteMode.OVERWRITE);

        LOG.info(env.getExecutionPlan());

        env.execute();

    }
}

// TODO:
//  - Add more metrics
//  - Workaround keyBy to control how data are partitioned.     In-Progress
