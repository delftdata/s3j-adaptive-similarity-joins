package StatFunctions;

import CustomDataTypes.GroupLevelShortOutput;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class LLCostCalculator extends ProcessWindowFunction<
        GroupLevelShortOutput, Tuple5<Long, Integer, Integer, Integer, Long>, Tuple3<Integer,Integer,Integer>, TimeWindow> {

    private final static MapStateDescriptor<Tuple2<Integer, Integer>, HashMap<String, Long>> mapDesc = new MapStateDescriptor<Tuple2<Integer,Integer>, HashMap<String, Long>>("stateStore",
            TypeInformation.of(new TypeHint<Tuple2<Integer,Integer>>() {}),
            TypeInformation.of(new TypeHint<HashMap<String, Long>>() {})
    );

    public LLCostCalculator(){
    }


    @Override
    public void process(Tuple3<Integer,Integer,Integer> key,
                        Context context, Iterable<GroupLevelShortOutput> tuples,
                        Collector<Tuple5<Long, Integer, Integer, Integer, Long>> collector) throws Exception {

        Long timestamp = null;
        HashMap<Integer, HashMap<Tuple2<Integer, Integer>, HashMap<String, Long>>> stats = new HashMap<>();
        HashMap<Integer, HashMap<Tuple2<Integer,Integer>, Long>> costs = new HashMap<>();

        MapState<Tuple2<Integer,Integer>, HashMap<String, Long>> stateStore = context.globalState().getMapState(mapDesc);

        for (GroupLevelShortOutput t : tuples) {
            Tuple2<Integer, Integer> groupKey = new Tuple2<>(t.f2, t.f3);
            if(!stats.containsKey(t.f1)){
                HashMap<String, Long> recordType = new HashMap<>();
                recordType.put("outlier", 0L);
                recordType.put("outer", 0L);
                recordType.put("inner", 0L);

                HashMap<Tuple2<Integer,Integer>, HashMap<String, Long>> logicalPart = new HashMap<>();
                logicalPart.put(groupKey, recordType);
                stats.put(t.f1, logicalPart);
            }
            else{
                if(!stats.get(t.f1).containsKey(groupKey)){
                    HashMap<String, Long> recordType = new HashMap<>();
                    recordType.put("outlier", 0L);
                    recordType.put("outer", 0L);
                    recordType.put("inner", 0L);

                    HashMap<Tuple2<Integer,Integer>, HashMap<String, Long>> logicalPart = stats.get(t.f1);
                    logicalPart.put(groupKey, recordType);
                    stats.put(t.f1, logicalPart);
                }
            }

            Long previous = 0L;
            HashMap<String, Long> recordType = stats.get(t.f1).get(groupKey);
            if(stateStore.contains(groupKey)){
                if(stateStore.get(groupKey).containsKey(t.f4)){
                    previous = stateStore.get(groupKey).get(t.f4);
                }
            }
            else{
                HashMap<String, Long> temp = new HashMap<>();
                temp.put("outlier", 0L);
                temp.put("outer", 0L);
                temp.put("inner", 0L);
                stateStore.put(groupKey, temp);
            }
            recordType.put(t.f4, t.f5 - previous);
            stateStore.get(groupKey).put(t.f4, t.f5);

            HashMap<Tuple2<Integer,Integer>, HashMap<String, Long>> logicalPart = stats.get(t.f1);
            logicalPart.put(groupKey, recordType);

            stats.put(t.f1, logicalPart);
            if(timestamp == null){
                timestamp = t.f0;
            }
        }

        for(Integer cm : stats.keySet()){
            for (Tuple2<Integer,Integer> lp : stats.get(cm).keySet()){
                HashMap<String, Long> recordTypes = stats.get(cm).get(lp);
                Long outliers = recordTypes.get("outlier");
                Long outers = recordTypes.get("outer");
                Long inners = recordTypes.get("inner");

                Long stateOutliers = stateStore.get(lp).get("outlier") - outliers;
                Long stateOuters = stateStore.get(lp).get("outer") - outers;
                Long stateInners = stateStore.get(lp).get("inner") - inners;

                Long cost = (outliers * (stateOuters + stateOutliers + stateInners)) + (outers * (stateInners + stateOutliers) + (inners * (stateOuters + stateOutliers)))
                        + (outliers * (outers + inners)) +  (outers * inners) + (outliers * (outliers-1))/2;

                if(!costs.containsKey(cm)){
                    HashMap<Tuple2<Integer,Integer>, Long> tmp = new HashMap<>();
                    costs.put(cm, tmp);
                }
                HashMap<Tuple2<Integer,Integer>, Long> lpCost = costs.get(cm);
                lpCost.put(lp, cost);
                costs.put(cm, lpCost);
            }
        }


        Tuple5<Long, Integer, Integer, Integer, Long> newTuple = null;
        for(Integer cm : costs.keySet()){
            for(Tuple2<Integer,Integer> lp : costs.get(cm).keySet()) {
                newTuple = new Tuple5<>(
                        timestamp,
                        cm,
                        lp.f0,
                        lp.f1,
                        costs.get(cm).get(lp)
                );
                collector.collect(newTuple);
            }
        }

    }
}
