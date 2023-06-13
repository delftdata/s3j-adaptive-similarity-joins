package Statistics;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class LLCostCalculator extends ProcessWindowFunction<Tuple5<Long, Integer, Integer, String, Long>, Tuple4<Long, Integer, Integer, Long>, Integer, TimeWindow> {

    private final static MapStateDescriptor<Integer, HashMap<String, Long>> mapDesc = new MapStateDescriptor<Integer, HashMap<String, Long>>("stateStore",
            TypeInformation.of(new TypeHint<Integer>() {}),
            TypeInformation.of(new TypeHint<HashMap<String, Long>>() {})
    );

    public LLCostCalculator(){
    }


    @Override
    public void process(Integer integer, Context context, Iterable<Tuple5<Long, Integer, Integer, String, Long>> tuples, Collector<Tuple4<Long, Integer, Integer, Long>> collector) throws Exception {

        Long timestamp = null;
        HashMap<Integer, HashMap<Integer, HashMap<String, Long>>> stats = new HashMap<>();
        HashMap<Integer, HashMap<Integer, Long>> costs = new HashMap<>();

        MapState<Integer, HashMap<String, Long>> stateStore = context.globalState().getMapState(mapDesc);

        for (Tuple5<Long, Integer, Integer, String, Long> t : tuples) {

            if(!stats.containsKey(t.f1)){
                HashMap<String, Long> recordType = new HashMap<>();
                recordType.put("outlier", 0L);
                recordType.put("outer", 0L);
                recordType.put("inner", 0L);

                HashMap<Integer, HashMap<String, Long>> logicalPart = new HashMap<>();
                logicalPart.put(t.f2, recordType);
                stats.put(t.f1, logicalPart);
            }
            else{
                if(!stats.get(t.f1).containsKey(t.f2)){
                    HashMap<String, Long> recordType = new HashMap<>();
                    recordType.put("outlier", 0L);
                    recordType.put("outer", 0L);
                    recordType.put("inner", 0L);

                    HashMap<Integer, HashMap<String, Long>> logicalPart = stats.get(t.f1);
                    logicalPart.put(t.f2, recordType);
                    stats.put(t.f1, logicalPart);
                }
            }

//            if(stateStore.contains(t.f2)){
//                HashMap<String, Long> recordType = stats.get(t.f1).get(t.f2);
//                for(String type : stateStore.get(t.f2).keySet()){
//                    recordType.put(type, stateStore.get(t.f2).get(type));
//                }
//            }
//            else{
//                stateStore.put(t.f2, new HashMap<String, Long>());
//            }
            Long previous = 0L;
            HashMap<String, Long> recordType = stats.get(t.f1).get(t.f2);
            if(stateStore.contains(t.f2)){
                if(stateStore.get(t.f2).containsKey(t.f3)){
                    previous = stateStore.get(t.f2).get(t.f3);
                }
            }
            else{
                HashMap<String, Long> temp = new HashMap<>();
                temp.put("outlier", 0L);
                temp.put("outer", 0L);
                temp.put("inner", 0L);
                stateStore.put(t.f2, temp);
            }
            recordType.put(t.f3, t.f4 - previous);
            stateStore.get(t.f2).put(t.f3, t.f4);

            HashMap<Integer, HashMap<String, Long>> logicalPart = stats.get(t.f1);
            logicalPart.put(t.f2, recordType);

            stats.put(t.f1, logicalPart);
            if(timestamp == null){
                timestamp = t.f0;
            }
        }

        for(Integer pp : stats.keySet()){
            for (Integer lp : stats.get(pp).keySet()){
                HashMap<String, Long> recordTypes = stats.get(pp).get(lp);
                Long outliers = recordTypes.get("outlier");
                Long outers = recordTypes.get("outer");
                Long inners = recordTypes.get("inner");

                Long stateOutliers = stateStore.get(lp).get("outlier") - outliers;
                Long stateOuters = stateStore.get(lp).get("outer") - outers;
                Long stateInners = stateStore.get(lp).get("inner") - inners;

                Long cost = (outliers * (stateOuters + stateOutliers + stateInners)) + (outers * (stateInners + stateOutliers) + (inners * (stateOuters + stateOutliers)))
                        + (outliers * (outers + inners)) +  (outers * inners) + (outliers * (outliers-1))/2;

                if(!costs.containsKey(pp)){
                    HashMap<Integer, Long> tmp = new HashMap<>();
                    costs.put(pp, tmp);
                }
                HashMap<Integer, Long> lpCost = costs.get(pp);
                lpCost.put(lp, cost);
                costs.put(pp, lpCost);
            }
        }

//        System.out.println(timestamp);
//        System.out.println(stats.toString());
//        System.out.println(tuples.toString());
//        System.out.println(costs);

        Tuple4<Long, Integer, Integer, Long> newTuple = null;
        for(Integer pp : costs.keySet()){
            for(Integer lp : costs.get(pp).keySet()) {
                newTuple = new Tuple4<>(
                        timestamp,
                        pp,
                        lp,
                        costs.get(pp).get(lp)
                );
                collector.collect(newTuple);
            }
        }

    }
}
