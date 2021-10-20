package Statistics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StatsMappers {

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
                        Tuple4<Integer, Integer, Boolean, Long>> {


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

    public static class TotalCostToList extends ProcessAllWindowFunction<Tuple3<Long,Integer,Long>,List<Tuple3<Long,Integer,Long>>, TimeWindow> {

        Integer p, maxP;

        public TotalCostToList(Integer p, Integer maxP){
            this.p = p;
            this.maxP = maxP;
        }

        @Override
        public void process(Context context, Iterable<Tuple3<Long, Integer, Long>> iterable, Collector<List<Tuple3<Long, Integer, Long>>> collector) throws Exception {
            HashMap<Integer, Long> costs = new HashMap<>();
            Integer pivot = 1+maxP/p;
            Long tmsp = null;

            for(int i=0; i<p ; i++){
                costs.put(i*pivot, 0L);
            }

            for(Tuple3<Long,Integer,Long> t : iterable){
                costs.put(t.f1, t.f2);
                tmsp = t.f0;
            }

            List<Tuple3<Long,Integer,Long>> out = new ArrayList<>();
            for(Integer key : costs.keySet()){
                out.add(
                        new Tuple3<>(
                                tmsp,
                                key,
                                costs.get(key)
                        )
                );
            }

            collector.collect(out);

        }
    }




}
