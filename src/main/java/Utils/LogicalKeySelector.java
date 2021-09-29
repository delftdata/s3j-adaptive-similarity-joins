package Utils;

import CustomDataTypes.FinalTuple;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

public class LogicalKeySelector implements KeySelector<FinalTuple, Tuple3<Integer, Integer, Integer>> {

    @Override
    public Tuple3<Integer, Integer, Integer> getKey(FinalTuple t) throws Exception {
        return new Tuple3<Integer, Integer, Integer>(t.f0, t.f10, t.f2);
    }
}
