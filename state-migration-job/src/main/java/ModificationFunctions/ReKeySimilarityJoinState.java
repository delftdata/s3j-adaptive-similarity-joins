package ModificationFunctions;

import CustomDataTypes.FinalTuple;
import CustomDataTypes.SimilarityJoinState;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.HashMap;

public class ReKeySimilarityJoinState implements MapFunction<SimilarityJoinState, SimilarityJoinState> {

    private final HashMap<Tuple3<Integer,Integer,Integer>, Integer> allocations;

    public ReKeySimilarityJoinState(HashMap<Tuple3<Integer,Integer,Integer>, Integer> allocations){
        this.allocations = allocations;
    }

    @Override
    public SimilarityJoinState map(SimilarityJoinState state) throws Exception {
        Tuple3<Integer,Integer,Integer> key = state.key;
        if(allocations.containsKey(key)){
            Integer machine = allocations.get(key);
            if(allocations.get(key) != key.f1) {
                state.key.f1 = machine;
                for (String outerKey : state.joinState.keySet()) {
                    for (String innerKey : state.joinState.get(outerKey).keySet()) {
                        for (FinalTuple t : state.joinState.get(outerKey).get(innerKey)) {
                            t.f10 = machine;
                        }
                    }
                }
            }
        }
        return state;
    }
}
