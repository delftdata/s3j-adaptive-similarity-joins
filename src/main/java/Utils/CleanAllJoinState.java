package Utils;

import CustomDataTypes.FinalTuple;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.KeyedStateFunction;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public class CleanAllJoinState implements KeyedStateFunction<Tuple3<Integer, Integer, Integer>, MapState<String, HashMap<String, List<FinalTuple>>>>, Serializable {

    @Override
    public void process(Tuple3<Integer, Integer, Integer> worksetKey, MapState<String, HashMap<String, List<FinalTuple>>> joinMapState) throws Exception {
        joinMapState.clear();
    }

}