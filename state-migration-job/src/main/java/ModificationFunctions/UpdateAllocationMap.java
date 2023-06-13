package ModificationFunctions;

import CustomDataTypes.GroupFormulationState;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.HashMap;

public class UpdateAllocationMap implements MapFunction<GroupFormulationState, GroupFormulationState> {

    private final HashMap<Tuple3<Integer,Integer,Integer>, Integer> allocations;

    public UpdateAllocationMap(HashMap<Tuple3<Integer,Integer,Integer>, Integer> allocations){
        this.allocations = allocations;
    }
    @Override
    public GroupFormulationState map(GroupFormulationState state) throws Exception {
        Integer key = state.key;
        for (Integer mappingKey : state.mapping.keySet()) {
            Tuple3<Integer, Integer, Integer> allocationsKey =
                    new Tuple3<>(mappingKey, state.mapping.get(mappingKey).f1, key);
            if (allocations.containsKey(allocationsKey)) {
                state.mapping.put(
                        mappingKey,
                        new Tuple2<>(state.mapping.get(mappingKey).f0, allocations.get(allocationsKey))
                );
            }
        }
        return state;
    }
}
