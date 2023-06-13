package StateReaderFunctions;

import CustomDataTypes.FinalTuple;
import CustomDataTypes.SimilarityJoinState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SimilarityJoinStateReader extends KeyedStateReaderFunction<Tuple3<Integer, Integer, Integer>, SimilarityJoinState> {

    MapState<String, HashMap<String, List<FinalTuple>>> joinState;

    @Override
    public void open(Configuration configuration) throws Exception {
        MapStateDescriptor<String, HashMap<String, List<FinalTuple>>> joinStateDesc =
                new MapStateDescriptor<String, HashMap<String, List<FinalTuple>>>(
                        "joinState",
                        TypeInformation.of(new TypeHint<String>() {}),
                        TypeInformation.of(new TypeHint<HashMap<String, List<FinalTuple>>>() {
                        })
                );
        joinState = getRuntimeContext().getMapState(joinStateDesc);
    }

    @Override
    public void readKey(Tuple3<Integer, Integer, Integer> key,
                        Context ctx,
                        Collector<SimilarityJoinState> out) throws Exception {

        SimilarityJoinState state = new SimilarityJoinState();
        state.key = key;
        state.joinState = toHashMap(joinState);

        out.collect(state);

    }

    public HashMap<String, HashMap<String, List<FinalTuple>>> toHashMap(
            MapState<String, HashMap<String, List<FinalTuple>>> mapState)
            throws Exception{

        HashMap<String, HashMap<String, List<FinalTuple>>> hm = new HashMap<>(
                StreamSupport
                        .stream(mapState.entries().spliterator(), false)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        );

        return hm;
    }
}
