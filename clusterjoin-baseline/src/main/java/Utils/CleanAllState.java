package Utils;

import CustomDataTypes.FinalTuple;
import CustomDataTypes.FinalTupleCJ;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.KeyedStateFunction;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public class CleanAllState implements KeyedStateFunction<Integer, MapState<String, HashMap<String, List<FinalTupleCJ>>>>, Serializable {

    @Override
    public void process(Integer partitionKey, MapState<String, HashMap<String, List<FinalTupleCJ>>> joinMapState) throws Exception {
        joinMapState.clear();
    }

}
