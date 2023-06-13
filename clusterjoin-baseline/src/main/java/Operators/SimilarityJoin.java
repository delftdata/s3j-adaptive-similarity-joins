package Operators;

import CustomDataTypes.FinalOutput;
import CustomDataTypes.FinalTuple;
import Utils.SimilarityJoinsUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimilarityJoin extends RichFlatMapFunction<FinalTuple, FinalOutput> {

    Double dist_thresh;
    private Logger LOG;
    OutputTag<Tuple3<Long, Integer, Integer>> sideJoins;
    MapState<String, HashMap<String, List<FinalTuple>>> joinState;
    private int counter;

    public SimilarityJoin(Double dist_thresh) throws Exception{
        this.dist_thresh = dist_thresh;
        this.LOG = LoggerFactory.getLogger(this.getClass().getName());
        this.counter = 0;
    }

    public void setSideJoins(OutputTag<Tuple3<Long, Integer, Integer>> sideJoins) {
        this.sideJoins = sideJoins;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, HashMap<String, List<FinalTuple>>> joinStateDesc =
                new MapStateDescriptor<String, HashMap<String, List<FinalTuple>>>(
                        "joinState",
                        TypeInformation.of(new TypeHint<String>() {}),
                        TypeInformation.of(new TypeHint<HashMap<String, List<FinalTuple>>>() {
                        })
                );

        joinState = getRuntimeContext().getMapState(joinStateDesc);

    }

    public boolean isSelfJoin() {
        return false;
    }

    @Override
    public void flatMap(FinalTuple incoming,
                        Collector<FinalOutput> collector)
            throws Exception {


        Double[] incomingEmbed = incoming.f9;
        String toCompare;
        if(incoming.f11.equals("left")){
            toCompare = "right";
        }
        else if (incoming.f11.equals("right")){
            toCompare = "left";
        } else {
            toCompare = "single";
        }

        LOG.info("Machine id: " + incoming.f10);
        long insertionStart = System.currentTimeMillis();
        insertToState(incoming);
        LOG.info("Insert to state took: " + (System.currentTimeMillis() - insertionStart));


        long retrieveStart = System.currentTimeMillis();
        List<FinalTuple> itemsToCompare = new ArrayList<>();
        List<FinalTuple> itemsToEmit = new ArrayList<>();
        if(joinState.contains(toCompare)) {
            HashMap<String, List<FinalTuple>> toCompareMap = joinState.get(toCompare);
            if (incoming.f1.equals("inner")) {
                itemsToCompare.addAll(toCompareMap.get("outer"));
                itemsToCompare.addAll(toCompareMap.get("outlier"));
                itemsToEmit.addAll(toCompareMap.get("inner"));
            } else if (incoming.f1.equals("outlier")) {
                itemsToCompare.addAll(toCompareMap.get("inner"));
                itemsToCompare.addAll(toCompareMap.get("outer"));
                itemsToCompare.addAll(toCompareMap.get("outlier"));
            } else {
                itemsToCompare.addAll(toCompareMap.get("inner"));
                itemsToCompare.addAll(toCompareMap.get("outlier"));
            }
            LOG.info("Retrieve from state took: " + (System.currentTimeMillis() - retrieveStart));


            long comparisonStart = System.currentTimeMillis();
            for (FinalTuple t : itemsToCompare) {

//            LOG.warn(incoming.toString()+", "+t.toString());

                if(isSelfJoin() && incoming.f8 == t.f8){
                    continue;
                }

                Double[] tEmbed = t.f9;
                if ((incoming.f8 > t.f8 && isSelfJoin()) || incoming.f11.equals("left")) {
                    boolean sim = SimilarityJoinsUtil.AngularDistance(incomingEmbed, tEmbed) < dist_thresh;
                    if (sim) {
                        collector.collect(
                                new FinalOutput(
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
                                    new FinalOutput(
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

                } else {
                    boolean sim = SimilarityJoinsUtil.AngularDistance(incomingEmbed, tEmbed) < dist_thresh;
                    if (sim) {
                        collector.collect(
                                new FinalOutput(
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
                                    new FinalOutput(
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
            LOG.info("Perform comparisons took: " + (System.currentTimeMillis() - comparisonStart));

            long groupedEmissionStart = System.currentTimeMillis();
            for(FinalTuple t : itemsToEmit){
//                LOG.warn(incoming.toString()+", "+t.toString());
                if(isSelfJoin() && incoming.f8 == t.f8){
                    continue;
                }
                if ((incoming.f8 > t.f8 && isSelfJoin()) || incoming.f11.equals("left")) {
                    collector.collect(
                            new FinalOutput(
                                    true,
                                    incoming,
                                    t,
                                    System.currentTimeMillis()
                            )
                    );
                } else {
                    collector.collect(
                            new FinalOutput(
                                    true,
                                    t,
                                    incoming,
                                    System.currentTimeMillis()
                            )
                    );
                }
            }
            LOG.info("Emitting items of the same group took: " + (System.currentTimeMillis() - groupedEmissionStart));

        }
    }



    private void insertToState(FinalTuple incoming) throws Exception {

        if(joinState.isEmpty()){
            HashMap<String, List<FinalTuple>> tmp = new HashMap<String, List<FinalTuple>>();
            List<FinalTuple> tmpList = new ArrayList<>();
            tmpList.add(incoming);
            tmp.put("inner", new ArrayList<>());
            tmp.put("outer", new ArrayList<>());
            tmp.put("outlier", new ArrayList<>());
            tmp.put(incoming.f1, tmpList);
            joinState.put(incoming.f11, tmp);
        }
        else{
            HashMap<String, List<FinalTuple>> tmp;
            if (joinState.contains(incoming.f11)){
                tmp = joinState.get(incoming.f11);
            }
            else{
                tmp = new HashMap<>();
                tmp.put("inner", new ArrayList<>());
                tmp.put("outer", new ArrayList<>());
                tmp.put("outlier", new ArrayList<>());
            }
            List<FinalTuple> tmpList;
            if(tmp.containsKey(incoming.f1)){
                tmpList = tmp.get(incoming.f1);
            }
            else{
                tmpList = new ArrayList<>();
            }
            tmpList.add(incoming);
            tmp.put(incoming.f1, tmpList);
            joinState.put(incoming.f11, tmp);
        }

    }

}