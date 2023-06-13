package Operators;

import CustomDataTypes.FinalOutput;
import CustomDataTypes.FinalOutputCJ;
import CustomDataTypes.FinalTuple;
import CustomDataTypes.FinalTupleCJ;
import Utils.CleanAllState;
import Utils.SimilarityJoinsUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SimilarityJoinCJ extends KeyedBroadcastProcessFunction<Integer,FinalTupleCJ,Integer,FinalOutputCJ>{

    Double dist_thresh;
    private Logger LOG;
    OutputTag<Tuple3<Long, Integer, Integer>> sideJoins;
    MapState<String, HashMap<String, List<FinalTupleCJ>>> joinState;
    MapStateDescriptor<String, HashMap<String, List<FinalTupleCJ>>> joinStateDesc;
    private int counter;

    public SimilarityJoinCJ(Double dist_thresh) throws Exception{
        this.dist_thresh = dist_thresh;
        this.LOG = LoggerFactory.getLogger(this.getClass().getName());
        this.counter = 0;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.joinStateDesc =
                new MapStateDescriptor<String, HashMap<String, List<FinalTupleCJ>>>(
                        "joinState",
                        TypeInformation.of(new TypeHint<String>() {}),
                        TypeInformation.of(new TypeHint<HashMap<String, List<FinalTupleCJ>>>() {
                        })
                );

        joinState = getRuntimeContext().getMapState(joinStateDesc);
    }

    @Override
    public void processElement(FinalTupleCJ incoming,
                        ReadOnlyContext ctx,
                        Collector<FinalOutputCJ> collector) throws Exception {

        Double[] incomingEmbed = incoming.f6;
        String toCompare;
        if(incoming.f7.equals("left")){
            toCompare = "right";
        }
        else if (incoming.f7.equals("right")){
            toCompare = "left";
        } else {
            toCompare = "single";
        }

        insertToState(incoming);

        List<FinalTupleCJ> itemsToCompare = new ArrayList<>();
        List<FinalTupleCJ> itemsToEmit = new ArrayList<>();
        if(joinState.contains(toCompare)) {
            HashMap<String, List<FinalTupleCJ>> toCompareMap = joinState.get(toCompare);
            if (incoming.f1.equals("pInner")) {
                itemsToCompare.addAll(toCompareMap.get("pOuter"));
                itemsToCompare.addAll(toCompareMap.get("pInner"));
            } else {
                itemsToCompare.addAll(toCompareMap.get("pInner"));
            }
        }


        for (FinalTupleCJ t : itemsToCompare) {
            if(isSelfJoin() && incoming.f5 == t.f5){
                continue;
            }

            Double[] tEmbed = t.f6;
            if ((incoming.f5 > t.f5 && isSelfJoin()) || incoming.f7.equals("left")) {
                boolean sim = SimilarityJoinsUtil.AngularDistance(incomingEmbed, tEmbed) < dist_thresh;
                if (sim) {
                    collector.collect(
                            new FinalOutputCJ(
                                    sim,
                                    incoming,
                                    t,
                                    System.currentTimeMillis()
                            )
                    );
                }
                else{
                    if (counter == 10000){
                        collector.collect(
                                new FinalOutputCJ(
                                        sim,
                                        incoming,
                                        t,
                                        System.currentTimeMillis()
                                )
                        );
                        counter = 0;
                    }
                    else {
                        counter++;
                    }
                }

            }
            else {
                boolean sim = SimilarityJoinsUtil.AngularDistance(incomingEmbed, tEmbed) < dist_thresh;
                if (sim) {
                    collector.collect(
                            new FinalOutputCJ(
                                    sim,
                                    t,
                                    incoming,
                                    System.currentTimeMillis()
                            )
                    );
                }
                else{
                    if(counter == 10000){
                        collector.collect(
                                new FinalOutputCJ(
                                        sim,
                                        t,
                                        incoming,
                                        System.currentTimeMillis()
                                )
                        );
                        counter = 0;
                    }
                    else {
                        counter++;
                    }
                }

            }
        }

    }

    @Override
    public void processBroadcastElement(Integer controlMsg, Context context, Collector<FinalOutputCJ> collector) throws Exception {
        context.applyToKeyedState(this.joinStateDesc, new CleanAllState());
    }

    public boolean isSelfJoin() {
        return false;
    }

    private void insertToState(FinalTupleCJ incoming) throws Exception {

        if(joinState.isEmpty()){
            HashMap<String, List<FinalTupleCJ>> tmp = new HashMap<String, List<FinalTupleCJ>>();
            List<FinalTupleCJ> tmpList = new ArrayList<>();
            tmpList.add(incoming);
            tmp.put("pInner", new ArrayList<>());
            tmp.put("pOuter", new ArrayList<>());
            tmp.put(incoming.f1, tmpList);
            joinState.put(incoming.f7, tmp);
        }
        else{
            HashMap<String, List<FinalTupleCJ>> tmp;
            if (joinState.contains(incoming.f7)){
                tmp = joinState.get(incoming.f7);
            }
            else{
                tmp = new HashMap<>();
                tmp.put("pInner", new ArrayList<>());
                tmp.put("pOuter", new ArrayList<>());
            }
            List<FinalTupleCJ> tmpList;
            if(tmp.containsKey(incoming.f1)){
                tmpList = tmp.get(incoming.f1);
            }
            else{
                tmpList = new ArrayList<>();
            }
            tmpList.add(incoming);
            tmp.put(incoming.f1, tmpList);
            joinState.put(incoming.f7, tmp);
        }

    }
}
