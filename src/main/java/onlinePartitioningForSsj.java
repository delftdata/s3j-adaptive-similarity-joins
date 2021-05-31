import org.apache.commons.math3.analysis.function.Sin;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.io.FileWriter;
import java.nio.file.Paths;
import java.util.*;


public class onlinePartitioningForSsj {

    private static final Logger LOG = LoggerFactory.getLogger(onlinePartitioningForSsj.class);

    static String pwd = Paths.get("").toAbsolutePath().toString();

    public static class CustomOnElementTrigger extends Trigger<Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>, GlobalWindow>{

        @Override
        public TriggerResult onElement(Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer> t, long l, GlobalWindow window, TriggerContext triggerContext) throws Exception {
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


    static public class CustomFiltering extends ProcessFunction<
            Tuple3<Boolean,
                    Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>,
                    Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[], Integer>>,
            Tuple3<Boolean,
                    Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>,
                    Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[], Integer>>> {

        OutputTag<Tuple4<Long, Boolean, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>>> sideStats;

        public CustomFiltering(
                OutputTag<Tuple4<Long, Boolean, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>>> sideStats){
            this.sideStats = sideStats;
        }

        @Override
        public void processElement(Tuple3<Boolean, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>> t,
                                   Context context, Collector<Tuple3<Boolean,
                Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>>> collector)
                throws Exception {

            if(t.f0){
                collector.collect(t);
            }
            if(t.f1.f6 > t.f2.f6){
                context.output(sideStats, new Tuple4<>(t.f1.f6, t.f0, t.f1, t.f2));
            }
            else{
                context.output(sideStats, new Tuple4<>(t.f2.f6, t.f0, t.f1, t.f2));
            }
        }
    }


    static public class WordToEmbeddingMapper implements MapFunction<Tuple3<Long, Integer, String>, Tuple3<Long, Integer, Double[]>>{

        HashMap<String, Double[]> wordEmbeddings;

        public WordToEmbeddingMapper(String filename) throws Exception{
            this.wordEmbeddings = SimilarityJoinsUtil.readEmbeddings(filename);
        }


        @Override
        public Tuple3<Long, Integer, Double[]> map(Tuple3<Long, Integer, String> t) throws Exception {
            return new Tuple3<>(t.f0, t.f1, wordEmbeddings.get(t.f2));
        }
    }



    // *****************************************************************************************************************

    // <---------------------------------------------- KEY SELECTORS -------------------------------------------------->

    // *****************************************************************************************************************


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

    public static class PhyStatsRtypeKeySelector implements KeySelector<Tuple3<Integer, String, Long>, Tuple2<Integer, String>>{

        @Override
        public Tuple2<Integer, String> getKey(Tuple3<Integer, String, Long> t) throws Exception {
            return new Tuple2<>(t.f0,t.f1);
        }
    }

    public static class LogicalKeySelector implements KeySelector<Tuple10<Integer,String,Integer, String, Integer, Integer, Long, Integer, Double[],Integer>, Tuple3<Integer, Integer, Integer>>{

        @Override
        public Tuple3<Integer, Integer, Integer> getKey(Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer> t) throws Exception {
            return new Tuple3<Integer, Integer, Integer>(t.f0, t.f9, t.f2);
        }
    }

    public static class BetweenPhyPartKeySelector implements KeySelector<Tuple4<Integer,Integer,Boolean,Long>,
            Tuple3<Integer, Integer, Boolean>>{


        @Override
        public Tuple3<Integer, Integer, Boolean> getKey(Tuple4<Integer, Integer, Boolean, Long> t) throws Exception {
            return new Tuple3<>(t.f0, t.f1, t.f2);
        }
    }

    public static class BetweenLogicalPartKeySelector implements KeySelector<Tuple5<Integer,Integer,Integer,Boolean,Long>,
            Tuple4<Integer, Integer, Integer, Boolean>>{


        @Override
        public Tuple4<Integer, Integer, Integer, Boolean> getKey(Tuple5<Integer, Integer, Integer, Boolean, Long> t) throws Exception {
            return new Tuple4<>(t.f0, t.f1, t.f2, t.f3);
        }
    }

    public static class RecordTypeLogicalPartitionSelector implements KeySelector<Tuple5<Long, Integer, Integer, String, Long>,
            Tuple3<Integer, Integer, String>>{


        @Override
        public Tuple3<Integer, Integer, String> getKey(Tuple5<Long, Integer, Integer, String, Long> t) throws Exception {
            return new Tuple3<>(t.f1, t.f2, t.f3);
        }
    }


    // *****************************************************************************************************************

    // <---------------------------------------------- STATS CLASSES -------------------------------------------------->

    // *****************************************************************************************************************




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


    public static class PPRecTypeList implements MapFunction<Tuple3<Integer, String, Long>, List<Tuple3<String, String, String>>> {

        private HashMap<Integer, HashMap<String, Long>> stats = new HashMap<>();

        @Override
        public List<Tuple3<String, String, String>> map(Tuple3<Integer, String, Long> t) throws Exception {
            if(!stats.containsKey(t.f0)){
                HashMap<String, Long> temp = new HashMap<>();
                temp.put("pInner", 0L);
                temp.put("pOuter", 0L);
                stats.put(t.f0, temp);
            }
            HashMap<String, Long> upd = stats.get(t.f0);
            upd.put(t.f1, t.f2);
            stats.put(t.f0, upd);

            List<Tuple3<String, String, String>> out = new ArrayList<>();
            for(Integer i : stats.keySet()){
                out.add(new Tuple3<String, String, String>(
                        Integer.toString(i),
                        "pInner = "+ stats.get(i).get("pInner").toString(),
                        "pOuter = "+ stats.get(i).get("pOuter").toString()));
            }
            return out;
        }
    }

    public static class OverallPartitionSizeList implements MapFunction<Tuple2<Integer, Long>, List<Tuple2<String,String>>>{

        private HashMap<Integer, Long> stats = new HashMap<>();

        @Override
        public List<Tuple2<String, String>> map(Tuple2<Integer, Long> t) throws Exception {

            stats.put(t.f0,t.f1);

            List<Tuple2<String, String>> out = new ArrayList<>();
            for(Integer i : stats.keySet()){
                out.add(new Tuple2<String, String>(
                        Integer.toString(i),
                        stats.get(i).toString()));
            }
            return out;
        }

    }


    public static class WindowedOverallPartitionSizeList implements MapFunction<Tuple3<Long, Integer, Long>, List<Tuple3<String,String,String>>>{

        private HashMap<Integer, Tuple2<Long,Long>> stats = new HashMap<>();

        @Override
        public List<Tuple3<String, String, String>> map(Tuple3<Long, Integer, Long> t) throws Exception {

            stats.put(t.f1, new Tuple2<Long,Long>(t.f0,t.f2));

            List<Tuple3<String,String, String>> out = new ArrayList<>();
            for(Integer i : stats.keySet()){
                out.add(new Tuple3<String, String, String>(
                        Integer.toString(i),
                        stats.get(i).f0.toString(),
                        stats.get(i).f1.toString()));
            }
            return out;
        }

    }


    public  static class BetweenPhyPartMapper implements FlatMapFunction
            <Tuple4<Long, Boolean, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>>,
                    Tuple4<Integer, Integer, Boolean, Long>>{


        @Override
        public void flatMap(Tuple4<
                Long,
                Boolean,
                Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer>,
                Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer>> t,
                            Collector<Tuple4<Integer, Integer, Boolean, Long>> collector) throws Exception {


            if(t.f2.f3.equals("pInner") && t.f3.f3.equals("pOuter")){
                collector.collect(new Tuple4<Integer,Integer,Boolean,Long>(t.f2.f2, t.f3.f4, t.f1, 1L));

            }
            else if(t.f2.f3.equals("pOuter") && t.f3.f3.equals("pInner")){
                collector.collect(new Tuple4<Integer,Integer,Boolean,Long>(t.f3.f2, t.f2.f4, t.f1, 1L));
            }

        }
    }


    public static class BetweenPhyPartMatchesList implements MapFunction<Tuple4<Integer, Integer, Boolean, Long>, List<Tuple4<String, String, String, String>>> {

        private HashMap<Integer, HashMap<Integer, HashMap<Boolean, Long>>> stats = new HashMap<>();

        @Override
        public List<Tuple4<String, String, String, String>> map(Tuple4<Integer, Integer, Boolean, Long> t) throws Exception {

            HashMap<Boolean, Long> boolUpd = new HashMap<>();
            HashMap<Integer, HashMap<Boolean, Long>> intUpd = new HashMap<>();

            if(!stats.containsKey(t.f0)){
                HashMap<Integer, HashMap<Boolean,Long>> temp = new HashMap<>();
                HashMap<Boolean, Long> boolTemp = new HashMap<>();
                boolTemp.put(true, 0L);
                boolTemp.put(false, 0L);
                temp.put(t.f1, boolTemp);
                stats.put(t.f0, temp);
            }
            else{
                if(!stats.get(t.f0).containsKey(t.f1)){
                    HashMap<Integer, HashMap<Boolean,Long>> temp = stats.get(t.f0);
                    HashMap<Boolean, Long> boolTemp = new HashMap<>();
                    boolTemp.put(true, 0L);
                    boolTemp.put(false, 0L);
                    temp.put(t.f1, boolTemp);
                    stats.put(t.f0, temp);
                }
            }

            boolUpd = stats.get(t.f0).get(t.f1);
            boolUpd.put(t.f2, t.f3);
            intUpd = stats.get(t.f0);
            intUpd.put(t.f1, boolUpd);
            stats.put(t.f0, intUpd);

            List<Tuple4<String, String, String, String>> out = new ArrayList<>();
            try {
                for (Integer i : stats.keySet()) {
                    for (Integer j : stats.get(i).keySet()) {
                        out.add(new Tuple4<String, String, String, String>(
                                Integer.toString(i),
                                Integer.toString(j),
                                "True = " + stats.get(i).get(j).get(true).toString(),
                                "False = " + stats.get(i).get(j).get(false).toString()));
                    }
                }
            }
            catch (Exception e){
                System.out.println(e.getMessage());
                System.out.println(stats.toString());
                throw e;
            }
            return out;
        }
    }


    public static class BetweenLogicalMapper implements FlatMapFunction
            <Tuple4<Long, Boolean, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>>,
            Tuple5<Integer, Integer, Integer, Boolean, Long>>{

        @Override
        public void flatMap(Tuple4<
                Long,
                Boolean,
                Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer>,
                Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer>> t,
                            Collector<Tuple5<Integer, Integer, Integer, Boolean, Long>> collector) throws Exception {

            Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer> t1 = t.f2;
            Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer> t2 = t.f3;


            if(t1.f3.equals("pInner") && t2.f3.equals("pInner")){
                if (t1.f1.equals("inner") && t2.f1.equals("outer")){
                    collector.collect(new Tuple5<Integer,Integer,Integer,Boolean,Long>(t1.f4, t1.f0, t2.f5, t.f1, 1L));
                }
                else if (t1.f1.equals("outer") && t2.f1.equals("inner")){
                    collector.collect(new Tuple5<Integer,Integer,Integer,Boolean,Long>(t2.f4, t2.f0, t1.f5, t.f1, 1L));
                }
            }



        }
    }

    public static class BetweenLogicalPartMatchesList implements MapFunction<
            Tuple5<Integer, Integer, Integer, Boolean, Long>,
            List<Tuple5<String, String, String, String, String>>> {

        private HashMap<Integer, HashMap<Integer, HashMap<Integer, HashMap<Boolean, Long>>>> stats = new HashMap<>();

        @Override
        public List<Tuple5<String, String, String, String, String>> map(Tuple5<Integer, Integer, Integer, Boolean, Long> t) throws Exception {

            HashMap<Boolean, Long> boolUpd;
            HashMap<Integer, HashMap<Boolean, Long>> intUpd;
            HashMap<Integer, HashMap<Integer, HashMap<Boolean, Long>>> physical;

            if(!stats.containsKey(t.f0)){
                HashMap<Integer, HashMap<Integer, HashMap<Boolean, Long>>> phyTemp = new HashMap<>();
                HashMap<Integer, HashMap<Boolean,Long>> temp = new HashMap<>();
                HashMap<Boolean, Long> boolTemp = new HashMap<>();
                boolTemp.put(true, 0L);
                boolTemp.put(false, 0L);
                temp.put(t.f2, boolTemp);
                phyTemp.put(t.f1, temp);
                stats.put(t.f0, phyTemp);
            }
            else{
                if(!stats.get(t.f0).containsKey(t.f1)){
                    HashMap<Integer, HashMap<Integer, HashMap<Boolean, Long>>> phyTemp = stats.get(t.f0);
                    HashMap<Integer, HashMap<Boolean,Long>> temp = new HashMap<>();
                    HashMap<Boolean, Long> boolTemp = new HashMap<>();
                    boolTemp.put(true, 0L);
                    boolTemp.put(false, 0L);
                    temp.put(t.f2, boolTemp);
                    phyTemp.put(t.f1, temp);
                    stats.put(t.f0, phyTemp);
                }
                else{
                    if(!stats.get(t.f0).get(t.f1).containsKey(t.f2)){
                        HashMap<Integer, HashMap<Integer, HashMap<Boolean, Long>>> phyTemp = stats.get(t.f0);
                        HashMap<Integer, HashMap<Boolean,Long>> temp = phyTemp.get(t.f1);
                        HashMap<Boolean, Long> boolTemp = new HashMap<>();
                        boolTemp.put(true, 0L);
                        boolTemp.put(false, 0L);
                        temp.put(t.f2, boolTemp);
                        phyTemp.put(t.f1, temp);
                        stats.put(t.f0, phyTemp);
                    }
                }
            }

            boolUpd = stats.get(t.f0).get(t.f1).get(t.f2);
            boolUpd.put(t.f3, t.f4);
            intUpd = stats.get(t.f0).get(t.f1);
            intUpd.put(t.f2, boolUpd);
            physical = stats.get(t.f0);
            physical.put(t.f1, intUpd);
            stats.put(t.f0, physical);


            List<Tuple5<String, String, String, String, String>> out = new ArrayList<>();
            try {
                for (Integer i : stats.keySet()) {
                    for (Integer j : stats.get(i).keySet()) {
                        for (Integer k: stats.get(i).get(j).keySet()) {
                            out.add(new Tuple5<String, String, String, String, String>(
                                    Integer.toString(i),
                                    Integer.toString(j),
                                    Integer.toString(k),
                                    "True = " + stats.get(i).get(j).get(k).get(true).toString(),
                                    "False = " + stats.get(i).get(j).get(k).get(false).toString()));
                        }
                    }
                }
            }
            catch (Exception e){
                System.out.println(e.getMessage());
                System.out.println(stats.toString());
                throw e;
            }
            return out;
        }
    }


    public static class NumOfLogicalPartMapper implements MapFunction<Tuple2<Integer,Integer>, List<Tuple2<Integer,Integer>>>{

        private HashMap<Integer, Integer> stats = new HashMap<>();

        @Override
        public List<Tuple2<Integer,Integer>> map(Tuple2<Integer, Integer> t) throws Exception {
            if(!stats.containsKey(t.f0)){
                stats.put(t.f0, 0);
            }

            int sizePP = stats.get(t.f0);
            if(sizePP < t.f1){
                stats.put(t.f0, t.f1);
            }

            List<Tuple2<Integer, Integer>> out = new ArrayList<>();
            for(Integer i : stats.keySet()){
                out.add(new Tuple2<Integer, Integer>(
                        i,
                        stats.get(i)
                ));
            }
            return out;
        }
    }

    public static class windowedNumOfLogicalPartMapper implements MapFunction<Tuple3<Long,Integer,Integer>, List<Tuple3<Long,Integer,Integer>>>{

        private HashMap<Integer, Tuple2<Long,Integer>> stats = new HashMap<>();

        @Override
        public List<Tuple3<Long,Integer,Integer>> map(Tuple3<Long,Integer, Integer> t) throws Exception {
            if(!stats.containsKey(t.f1)){
                stats.put(t.f1, new Tuple2<Long, Integer>(0L,0));
            }

            Tuple2<Long,Integer> sizePP = stats.get(t.f1);
            if(sizePP.f1 <= t.f2){
                stats.put(t.f1, new Tuple2<>(t.f0, t.f2));
            }
            else{
                stats.put(t.f1, new Tuple2<>(t.f0, sizePP.f1));
            }


            List<Tuple3<Long, Integer, Integer>> out = new ArrayList<>();
            for(Integer i : stats.keySet()){
                out.add(new Tuple3<Long, Integer, Integer>(
                        stats.get(i).f0,
                        i,
                        stats.get(i).f1
                ));
            }
            return out;
        }
    }

    public static class windowedComparisonsByLogicalPartMapper implements MapFunction<Tuple3<Long,Integer,Integer>, List<Tuple3<Long,Integer,Integer>>>{

        private HashMap<Integer, Tuple2<Long,Integer>> stats = new HashMap<>();

        @Override
        public List<Tuple3<Long,Integer,Integer>> map(Tuple3<Long,Integer, Integer> t) throws Exception {
            if(!stats.containsKey(t.f1)){
                stats.put(t.f1, new Tuple2<Long, Integer>(0L,0));
            }

            Tuple2<Long,Integer> sizePP = stats.get(t.f1);
            if(sizePP.f1 <= t.f2){
                stats.put(t.f1, new Tuple2<>(t.f0, t.f2));
            }
            else{
                stats.put(t.f1, new Tuple2<>(t.f0, sizePP.f1));
            }


            List<Tuple3<Long, Integer, Integer>> out = new ArrayList<>();
            for(Integer i : stats.keySet()){
                out.add(new Tuple3<Long, Integer, Integer>(
                        stats.get(i).f0,
                        i,
                        stats.get(i).f1
                ));
            }
            return out;
        }
    }


    public static class windowedComparisonsPerPhyPartMapper implements MapFunction<Tuple3<Long,Integer,Long>, List<Tuple3<Long,Integer,Long>>>{

        private HashMap<Integer, Tuple2<Long,Long>> stats = new HashMap<>();

        @Override
        public List<Tuple3<Long,Integer,Long>> map(Tuple3<Long,Integer, Long> t) throws Exception {

            stats.put(t.f1, new Tuple2<>(t.f0, t.f2));

            List<Tuple3<Long, Integer, Long>> out = new ArrayList<>();
            for(Integer i : stats.keySet()){
                out.add(new Tuple3<Long, Integer, Long>(
                        stats.get(i).f0,
                        i,
                        stats.get(i).f1
                ));
            }
            return out;
        }
    }

    public static class RecordTypeLPMapper implements MapFunction<Tuple5<Long,Integer,Integer,String,Long>, Tuple5<Long,Integer,Integer,String,Long>>{

        private HashMap<Tuple3<Integer,Integer,String>, Tuple2<Long,Long>> stats = new HashMap<>();

        @Override
        public Tuple5<Long, Integer, Integer, String, Long> map(Tuple5<Long, Integer, Integer, String, Long> t) throws Exception {
            Tuple3<Integer,Integer,String> key = new Tuple3<>(t.f1,t.f2,t.f3);
            if (!stats.containsKey(key)){
                stats.put(key, new Tuple2<>(0L,0L));
            }

            Tuple2<Long, Long> countWithTime = stats.get(key);
            Long nCount = countWithTime.f1+t.f4;
            stats.put(key, new Tuple2<>(t.f0, nCount));

            return new Tuple5<>(t.f0, t.f1, t.f2, t.f3, nCount);
        }
    }


    public static class RecordTypeLPList implements MapFunction<Tuple5<Long,Integer,Integer,String,Long>, List<Tuple5<Long,Integer,Integer,String,Long>>>{

        private HashMap<Tuple3<Integer,Integer,String>, Tuple2<Long,Long>> stats = new HashMap<>();

        @Override
        public List<Tuple5<Long, Integer, Integer, String, Long>> map(Tuple5<Long, Integer, Integer, String, Long> t) throws Exception {
            Tuple3<Integer, Integer, String> key = new Tuple3<>(t.f1, t.f2, t.f3);

            stats.put(key, new Tuple2<>(t.f0, t.f4));

            List<Tuple5<Long, Integer, Integer, String, Long>> out = new ArrayList<>();
            for (Tuple3<Integer, Integer, String> i : stats.keySet()) {
                out.add(new Tuple5<Long, Integer, Integer, String, Long>(
                        stats.get(i).f0,
                        i.f0,
                        i.f1,
                        i.f2,
                        stats.get(i).f1
                ));
            }
            return out;
        }
    }


    public static class LogicalPartitionCentroidsList implements MapFunction<
            Tuple2<Integer, HashMap<Integer, Tuple3<Long, Integer, Double[]>>>,
            List<Tuple2<Integer, HashMap<Integer, Tuple3<Long, Integer, Double[]>>>>>{

        HashMap<Integer, HashMap<Integer, Tuple3<Long, Integer, Double[]>>> centroids = new HashMap<>();
        @Override
        public List<Tuple2<Integer, HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> map(
                Tuple2<Integer, HashMap<Integer, Tuple3<Long, Integer, Double[]>>> t) throws Exception {

            centroids.put(t.f0, t.f1);
            ArrayList<Tuple2<Integer, HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> out = new ArrayList<>();
            for(Integer i : centroids.keySet()){
                out.add(
                        new Tuple2<>(
                                i,
                                centroids.get(i)
                        )
                );
            }

            return out;
        }
    }



    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamFactory streamFactory = new StreamFactory(env);
        env.setMaxParallelism(128);
        env.setParallelism(10);

        LOG.info("Enter main.");

//        final OutputTag<Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> sideLCentroids =
//                new OutputTag<Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>>("logicalCentroids"){};

        final OutputTag<Tuple3<Long, Integer, Integer>> sideLP =
                new OutputTag<Tuple3<Long, Integer, Integer>>("logicalPartitions"){};

        final OutputTag<Tuple3<Long, Integer, Integer>> sideJoins =
                new OutputTag<Tuple3<Long, Integer, Integer>>("joinComputations"){};

        env.setParallelism(1);

        DataStream<Tuple3<Long, Integer, Double[]>> data = streamFactory.create2DArrayStream("1K_2D_Array_Stream_v2.txt");
//        data.print();
//        DataStream<Tuple3<Long, Integer, Double[]>> embeddedData = data.map(new WordToEmbeddingMapper("wiki-news-300d-1K.vec"));

        env.setParallelism(10);

        DataStream<Tuple6<Integer,String,Integer,Long,Integer,Double[]>> ppData = data.
                flatMap(new PhysicalPartitioner(0.1, SimilarityJoinsUtil.RandomCentroids(10, 2), (env.getMaxParallelism()/env.getParallelism())+1));


        SingleOutputStreamOperator<Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>> lpData = ppData
                .keyBy(t -> t.f0)
                .process(new AdaptivePartitioner(0.1, (env.getMaxParallelism()/env.getParallelism())+1, LOG, sideLP));

        final OutputTag<Tuple4<Long, Boolean, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>>> sideStats =
                new OutputTag<Tuple4<Long, Boolean, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>>>("stats"){};

        SingleOutputStreamOperator<Tuple3<Boolean, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>,Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>>>
                unfilteredSelfJoinedStream = lpData
                .keyBy(new LogicalKeySelector())
                .window(GlobalWindows.create())
                .trigger(new CustomOnElementTrigger())
                .process(new SimilarityJoin(0.1, LOG, sideJoins));

        SingleOutputStreamOperator<Tuple3<Boolean, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>,Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>>>
                selfJoinedStream = unfilteredSelfJoinedStream
                .process(new CustomFiltering(sideStats));

//        selfJoinedStream.print();


//*********************************************      STATISTICS SECTION      *******************************************

        env.setParallelism(1);


        //<-------  Records labeled with partition ids ---------->
        ppData.writeAsText(pwd+"/src/main/outputs/PhysicalPartitioning.txt", FileSystem.WriteMode.OVERWRITE);

        //<------- Records labeled with logical partition ids ---------->
        lpData.writeAsText(pwd+"/src/main/outputs/LogicalPartitioning.txt", FileSystem.WriteMode.OVERWRITE);

//        //<--------- Logical Partitions and centroids --------->
//        lpData.getSideOutput(sideLCentroids)
//                .map(new LogicalPartitionCentroidsList())
//                .addSink(new SinkFunction<List<Tuple2<Integer, HashMap<Integer, Tuple3<Long, Integer, Double[]>>>>>() {
//                    @Override
//                    public void invoke(List<Tuple2<Integer, HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> value, Context context) throws Exception {
//                        FileWriter myWriter = new FileWriter(pwd+"/src/main/outputs/LogicalPartitionCentroids.txt");
//                        myWriter.write(value.toString());
//                        myWriter.close();
//                    }
//                });

        //<-------  Capture the size of physical partitions --------->
        ppData
                .map(t -> new Tuple2<>(t.f0, 1L))
                .returns(TypeInformation.of((new TypeHint<Tuple2<Integer, Long>>() {
                })))
                .keyBy(t -> t.f0)
                .sum(1)
                .map(new OverallPartitionSizeList())
                .addSink(new SinkFunction<List<Tuple2<String, String>>>() {
                    @Override
                    public void invoke(List<Tuple2<String, String>> value, Context context) throws Exception {
                        FileWriter myWriter = new FileWriter(pwd+"/src/main/outputs/physicalPartitionSizes.txt");
                        myWriter.write(value.toString());
                        myWriter.close();
                    }
                });



        //<-------  Capture the size of physical partitions per window --------->
        OutputTag<Tuple3<Long, Integer, Long>> latePP = new OutputTag<Tuple3<Long, Integer, Long>>("latePP"){};
        SingleOutputStreamOperator<Tuple3<Long, Integer, Long>> incRatePerWindow = ppData
                .map(t -> new Tuple3<>(t.f3, t.f0, 1L))
                .returns(TypeInformation.of((new TypeHint<Tuple3<Long, Integer, Long>>() {
                })))
                .keyBy(t -> t.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .sideOutputLateData(latePP)
                .sum(2);

        incRatePerWindow.getSideOutput(latePP).print();

        incRatePerWindow.map(new WindowedOverallPartitionSizeList()).writeAsText(pwd + "/src/main/outputs/windowedPhysicalPartitionSizes.txt", FileSystem.WriteMode.OVERWRITE);



        //<-------  Capture the number of inner and outer records in each physical partition --------->
        ppData
                .map(t -> new Tuple3<>(t.f0, t.f1, 1L))
                .returns(TypeInformation.of(new TypeHint<Tuple3<Integer, String, Long>>() {
                }))
                .keyBy(new PhyStatsRtypeKeySelector())
                .sum(2)
                .map(new PPRecTypeList())
                .addSink(new SinkFunction<List<Tuple3<String, String, String>>>() {
                    @Override
                    public void invoke(List<Tuple3<String, String, String>> value, Context context) throws Exception {
                        FileWriter myWriter = new FileWriter(pwd+"/src/main/outputs/ppRecordTypes.txt");
                        myWriter.write(value.toString());
                        myWriter.close();
                    }
                });



        //<-------  Capture the number of true and false comparisons in each logical partition --------->
        selfJoinedStream.getSideOutput(sideStats)
                .map(t -> new Tuple4<>(t.f2.f2, t.f2.f0, t.f1, 1L))
                .returns((TypeInformation.of(new TypeHint<Tuple4<Integer, Integer, Boolean, Long>>() {
                })))
                .keyBy(new StatsKeySelector())
                .sum(3)
                .map(new LPMeasurePerformance())
                .addSink(new SinkFunction<List<Tuple4<String, String, String, String>>>() {
                    @Override
                    public void invoke(List<Tuple4<String, String, String, String>> value, Context context) throws Exception {
                        FileWriter myWriter = new FileWriter(pwd+"/src/main/outputs/comparisonsByLogicalPart.txt");
                        myWriter.write(value.toString());
                        myWriter.close();
                    }
                });



        //<-------  Capture the number of true and false comparisons in each physical partition --------->
        selfJoinedStream.getSideOutput(sideStats)
                .map(t -> new Tuple3<>(t.f2.f2, t.f1, 1L))
                .returns(TypeInformation.of(new TypeHint<Tuple3<Integer, Boolean, Long>>() {
                }))
                .keyBy(new PhyStatsKeySelector())
                .sum(2)
                .map(new PPMeasurePerformance())
                .addSink(new SinkFunction<List<Tuple3<String, String, String>>>() {
                    @Override
                    public void invoke(List<Tuple3<String, String, String>> value, Context context) throws Exception {
                        FileWriter myWriter = new FileWriter(pwd+"/src/main/outputs/comparisonsByPhysicalPart.txt");
                        myWriter.write(value.toString());
                        myWriter.close();
                    }
                });



        //<-------  Capture the number of true and false comparisons between physical partitions --------->
        selfJoinedStream.getSideOutput(sideStats)
                .flatMap(new BetweenPhyPartMapper())
                .keyBy(new BetweenPhyPartKeySelector())
                .sum(3)
                .map(new BetweenPhyPartMatchesList())
                .addSink(new SinkFunction<List<Tuple4<String, String, String, String>>>() {
                    @Override
                    public void invoke(List<Tuple4<String, String, String, String>> value, Context context) throws Exception {
                        FileWriter myWriter = new FileWriter(pwd+"/src/main/outputs/matchesBetweenPhysicalPart.txt");
                        myWriter.write(value.toString());
                        myWriter.close();
                    }
                });


        //<------- Capture the number of true and false comparisons between logical partitions --------->
        selfJoinedStream.getSideOutput(sideStats)
                .flatMap(new BetweenLogicalMapper())
                .keyBy(new BetweenLogicalPartKeySelector())
                .sum(4)
                .map(new BetweenLogicalPartMatchesList())
                .addSink(new SinkFunction<List<Tuple5<String, String, String, String, String>>>() {
                    @Override
                    public void invoke(List<Tuple5<String, String, String, String, String>> value, Context context) throws Exception {
                        FileWriter myWriter = new FileWriter(pwd+"/src/main/outputs/matchesBetweenLogicalPart.txt");
                        myWriter.write(value.toString());
                        myWriter.close();
                    }
                });



        //<-------- Number of logical partitions within each physical ---------->
        lpData
                .map(t -> new Tuple2<Integer,Integer>(t.f2, t.f0))
                .returns(TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {}))
                .map(new NumOfLogicalPartMapper())
                .addSink(new SinkFunction<List<Tuple2<Integer, Integer>>>() {
                    @Override
                    public void invoke(List<Tuple2<Integer, Integer>> value, Context context) throws Exception {
                        FileWriter myWriter = new FileWriter(pwd+"/src/main/outputs/NumOfLogicalPartPerPhysical.txt");
                        myWriter.write(value.toString());
                        myWriter.close();
                    }
                });


        //<------- Number of logical partitions within each physical per window ----->
        OutputTag<Tuple3<Long, Integer, Integer>> lateLP = new OutputTag<Tuple3<Long, Integer, Integer>>("lateLP"){};
        SingleOutputStreamOperator<Tuple3<Long, Integer, Integer>> wd = lpData.getSideOutput(sideLP)
                .keyBy(t -> t.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .sideOutputLateData(lateLP)
                .max(1);

        wd.getSideOutput(lateLP).print();

        wd.map(new windowedNumOfLogicalPartMapper())
                .writeAsText(pwd+"/src/main/outputs/windowedNumOfLogicalPartPerPhysical.txt", FileSystem.WriteMode.OVERWRITE);


        //<-------- Record Types in Logical Partitions per window -------->
        OutputTag<Tuple5<Long, Integer, Integer, String, Long>> lateRTLP = new OutputTag<Tuple5<Long, Integer, Integer, String, Long>>("lateRTLP"){};
        SingleOutputStreamOperator<Tuple5<Long, Integer, Integer, String, Long>> rtlp =
                lpData
                .map(t -> new Tuple5<>(t.f6, t.f2, t.f0, t.f1, 1L))
                .returns(TypeInformation.of(new TypeHint<Tuple5<Long, Integer, Integer, String, Long>>() {
                }))
                .keyBy(new RecordTypeLogicalPartitionSelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .sideOutputLateData(lateRTLP)
                .sum(4);

        rtlp.getSideOutput(lateRTLP).print();
        rtlp.map(new RecordTypeLPMapper()).map(new RecordTypeLPList()).writeAsText(pwd+"/src/main/outputs/LogicalPartitionSizePerWindow.txt", FileSystem.WriteMode.OVERWRITE);


        //<--------- Cost calculation per logical partition ----------->

        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, Long>> listCosts =
            rtlp.map(new RecordTypeLPMapper())
                .keyBy(t -> t.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .process(new LLCostCalculator());

        listCosts.writeAsText(pwd+"/src/main/outputs/LogicalPartitionCostPerWindow.txt", FileSystem.WriteMode.OVERWRITE);


        //<----------- Mapping cost for logical level ----------->
        DataStream<Tuple3<Long, Integer, Long>> mappingCostLL = wd.join(incRatePerWindow)
                .where(t -> t.f1)
                .equalTo(t -> t.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new JoinFunction<Tuple3<Long, Integer, Integer>, Tuple3<Long, Integer, Long>, Tuple3<Long, Integer, Long>>() {
                    @Override
                    public Tuple3<Long, Integer, Long> join(Tuple3<Long, Integer, Integer> rateTuple, Tuple3<Long, Integer, Long> llsizeTuple) throws Exception {
                        Long cost = rateTuple.f2 * llsizeTuple.f2;
                        return new Tuple3<Long, Integer, Long>(rateTuple.f0, rateTuple.f1, cost);
                    }
                });

//        mappingCostLL.print();


        //<----------- Total cost per machine -------->
        DataStream<Tuple3<Long, Integer, Long>> totalCosts =
                listCosts
                        .map(t -> new Tuple3<>(t.f0, t.f1, t.f3))
                        .returns(TypeInformation.of(new TypeHint<Tuple3<Long, Integer, Long>>() {
                        }))
                        .keyBy(t -> t.f1)
                        .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                        .sum(2)
                        .join(mappingCostLL)
                        .where(t -> t.f1)
                        .equalTo(t -> t.f1)
                        .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                        .apply(new JoinFunction<Tuple3<Long, Integer, Long>, Tuple3<Long, Integer, Long>, Tuple3<Long, Integer, Long>>() {
                            @Override
                            public Tuple3<Long, Integer, Long> join(Tuple3<Long, Integer, Long> joinCost, Tuple3<Long, Integer, Long> mappingCost) throws Exception {
                                Long cost = joinCost.f2 + mappingCost.f2;
                                return new Tuple3<>(joinCost.f0, joinCost.f1, cost);
                            }
                        });



        totalCosts.writeAsText(pwd+"/src/main/outputs/windowedTotalCostPerMachine.txt", FileSystem.WriteMode.OVERWRITE);


//          CHECK WITH MARIOS



//        //<------- comparisons by logical partition per window -------->
//        selfJoinedStream.getSideOutput(sideStats)
//                .map(t -> new Tuple4<>(t.f0, t.f2.f2, t.f2.f0, 1L))
//                .returns(TypeInformation.of(new TypeHint<Tuple4<Long, Integer, Integer, Long>>() {
//                }))
//                .keyBy(t -> new Tuple2<Integer,Integer>(t.f1, t.f2))
//                .window(TumblingEventTimeWindows.of(Time.milliseconds(1)))
//                .sum(3);

//        //<------- comparisons by physical partition per window --------->
//        OutputTag<Tuple3<Long, Integer, Long>> lateJoin = new OutputTag<Tuple3<Long, Integer, Long>>("lateJoin"){};
//        SingleOutputStreamOperator<Tuple3<Long,Integer,Long>> check =
//        unfilteredSelfJoinedStream.getSideOutput(sideJoins)
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Integer, Integer>>() {
//                    @Override
//                    public long extractAscendingTimestamp(Tuple3<Long, Integer, Integer> t) {
//                        return t.f0;
//                    }
//                })
//                .map(t -> new Tuple3<Long, Integer, Long>(t.f0, t.f1, 1L))
//                .returns(TypeInformation.of(new TypeHint<Tuple3<Long, Integer, Long>>() {
//                }))
//                .keyBy(t -> t.f1)
//                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
//                .sideOutputLateData(lateJoin)
//                .sum(2);

//        check.getSideOutput(lateJoin).print();

//        check
//                .map(new windowedComparisonsPerPhyPartMapper())
//                .writeAsText(pwd+"/src/main/outputs/windowedComparisonsPerPhysical.txt", FileSystem.WriteMode.OVERWRITE);



        LOG.info(env.getExecutionPlan());

        env.execute();


    }
}

// TODO:
//  - Use embeddings from end-to-end.  IN-PROGRESS
//  - Add more metrics      IN-PROGRESS
//  - Create Tests.       DONE
//  - Workaround keyBy to control how data are partitioned.     DONE
