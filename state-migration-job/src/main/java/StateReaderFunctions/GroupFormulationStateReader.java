package StateReaderFunctions;

import CustomDataTypes.GroupFormulationState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GroupFormulationStateReader extends KeyedStateReaderFunction<Integer, GroupFormulationState> {

    MapState<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> mappingGroupsToNodes;
    ListState<Tuple8<Integer,String,Integer,Long, Long,Integer,Double[],String>> phyOuters;

    @Override
    public void open(Configuration configuration) throws Exception {
        MapStateDescriptor<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> mapStateDescriptor =
                new MapStateDescriptor<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>>(
                        "mapping",
                        TypeInformation.of(Integer.class),
                        TypeInformation.of(new TypeHint<Tuple2<Tuple3<Long, Integer, Double[]>, Integer>>() {})
                );
        mappingGroupsToNodes = getRuntimeContext().getMapState(mapStateDescriptor);

        ListStateDescriptor<Tuple8<Integer,String,Integer,Long, Long,Integer,Double[],String>> listStateDescriptor =
                new ListStateDescriptor<Tuple8<Integer, String, Integer, Long, Long, Integer, Double[], String>>(
                        "phyOuters",
                        TypeInformation.of(new TypeHint<Tuple8<Integer, String, Integer, Long, Long, Integer, Double[], String>>() {})
                );
        phyOuters = getRuntimeContext().getListState(listStateDescriptor);
    }

    @Override
    public void readKey(Integer key,
                        Context ctx,
                        Collector<GroupFormulationState> out) throws Exception {

        GroupFormulationState state = new GroupFormulationState();
        state.key = key;
        state.mapping = toHashMap(mappingGroupsToNodes);
        state.physicalOuters = toList(phyOuters);

        out.collect(state);

    }

    public HashMap<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> toHashMap(
            MapState<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> mapState)
            throws Exception{

        HashMap<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> hm = new HashMap<>(
                StreamSupport
                        .stream(mapState.entries().spliterator(), false)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        );

        return hm;
    }

    public List<Tuple8<Integer,String,Integer,Long, Long,Integer,Double[],String>> toList(
            ListState<Tuple8<Integer,String,Integer,Long, Long,Integer,Double[],String>> listState)
            throws Exception{

        return StreamSupport
                .stream(listState.get().spliterator(), false)
                .collect(Collectors.toList());
    }
}
