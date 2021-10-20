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
import java.util.List;

import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimilarityJoin extends RichFlatMapFunction<FinalTuple, FinalOutput> {

    Double dist_thresh;
    private Logger LOG;
    OutputTag<Tuple3<Long, Integer, Integer>> sideJoins;
    private MapState<String, List<FinalTuple>> joinState;

    public SimilarityJoin(Double dist_thresh) throws Exception{
        this.dist_thresh = dist_thresh;
        this.LOG = LoggerFactory.getLogger(this.getClass().getName());
    }

    public void setSideJoins(OutputTag<Tuple3<Long, Integer, Integer>> sideJoins) {
        this.sideJoins = sideJoins;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, List<FinalTuple>> joinStateDesc =
                new MapStateDescriptor<String, List<FinalTuple>>(
                        "joinState",
                        TypeInformation.of(new TypeHint<String>() {}),
                        TypeInformation.of(new TypeHint<List<FinalTuple>>() {
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

        insertToState(incoming);

        List<FinalTuple> itemsToCompare = new ArrayList<>();
        List<FinalTuple> itemsToEmit = new ArrayList<>();

        if(!joinState.isEmpty()) {
            if (incoming.f1.equals("inner")) {
                itemsToCompare.addAll(joinState.get("outer"));
                itemsToCompare.addAll(joinState.get("outlier"));
                itemsToEmit.addAll(joinState.get("inner"));
            } else if (incoming.f1.equals("outlier")) {
                itemsToCompare.addAll(joinState.get("inner"));
                itemsToCompare.addAll(joinState.get("outer"));
                itemsToCompare.addAll(joinState.get("outlier"));
            } else {
                itemsToCompare.addAll(joinState.get("inner"));
                itemsToCompare.addAll(joinState.get("outlier"));
            }
        }

        for (FinalTuple t : itemsToCompare) {

//            LOG.warn(incoming.toString()+", "+t.toString());

            if(isSelfJoin() && incoming.f8.equals(t.f8)){
                continue;
            }

            Double[] tEmbed = t.f9;

            if (incoming.f11.equals("left")) {
                collector.collect(
                        new FinalOutput(
                                (SimilarityJoinsUtil.AngularDistance(incomingEmbed, tEmbed) < dist_thresh),
                                incoming,
                                t,
                                System.currentTimeMillis()
                        )
                );
            } else {
                collector.collect(
                        new FinalOutput(
                                (SimilarityJoinsUtil.AngularDistance(incomingEmbed, tEmbed) < dist_thresh),
                                t,
                                incoming,
                                System.currentTimeMillis()
                        )
                );
            }
        }

        for(FinalTuple t : itemsToEmit){
//                LOG.warn(incoming.toString()+", "+t.toString());
            if(isSelfJoin() && incoming.f8.equals(t.f8)){
                continue;
            }
            if (incoming.f11.equals("left")) {
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
    }



    private void insertToState(FinalTuple incoming) throws Exception {

        if(joinState.isEmpty()){
            List<FinalTuple> tmpList = new ArrayList<>();
            tmpList.add(incoming);
            joinState.put("inner", new ArrayList<>());
            joinState.put("outer", new ArrayList<>());
            joinState.put("outlier", new ArrayList<>());
            joinState.put(incoming.f1, tmpList);
        }
        else{
            List<FinalTuple> tmpList;
            tmpList = joinState.get(incoming.f1);
            tmpList.add(incoming);
            joinState.put(incoming.f1, tmpList);
        }

    }

}