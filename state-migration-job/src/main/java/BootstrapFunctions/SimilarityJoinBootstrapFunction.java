package BootstrapFunctions;

import CustomDataTypes.FinalTuple;
import CustomDataTypes.SimilarityJoinState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;

import java.util.HashMap;
import java.util.List;

public class SimilarityJoinBootstrapFunction extends KeyedStateBootstrapFunction<
        Tuple3<Integer, Integer, Integer>,
        SimilarityJoinState> {

    MapState<String, HashMap<String, List<FinalTuple>>> joinState;

    @Override
    public void open(Configuration parameters){
        MapStateDescriptor<String, HashMap<String, List<FinalTuple>>> joinStateDesc =
                new MapStateDescriptor<String, HashMap<String, List<FinalTuple>>>(
                        "joinState",
                        TypeInformation.of(String.class),
                        TypeInformation.of(new TypeHint<HashMap<String, List<FinalTuple>>>() {
                        })
                );
        joinState = getRuntimeContext().getMapState(joinStateDesc);
    }

    @Override
    public void processElement(SimilarityJoinState state, KeyedStateBootstrapFunction<Tuple3<Integer, Integer, Integer>, SimilarityJoinState>.Context ctx) throws Exception {
        this.joinState.putAll(state.joinState);
    }
}
