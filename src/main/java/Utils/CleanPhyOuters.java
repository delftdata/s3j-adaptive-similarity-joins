package Utils;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.runtime.state.KeyedStateFunction;

public class CleanPhyOuters implements KeyedStateFunction<Integer,
        ListState<Tuple8<Integer,String,Integer,Long, Long,Integer,Double[],String>>> {
    @Override
    public void process(Integer integer, ListState<Tuple8<Integer, String, Integer, Long, Long, Integer, Double[], String>> phyOuters) throws Exception {
        phyOuters.clear();
    }
}
