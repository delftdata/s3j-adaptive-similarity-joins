package Utils;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple3;

public class LogicalKeySelector implements KeySelector<Tuple10<Integer,String,Integer, String, Integer, Integer, Long, Integer, Double[],Integer>, Tuple3<Integer, Integer, Integer>> {

    @Override
    public Tuple3<Integer, Integer, Integer> getKey(Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer> t) throws Exception {
        return new Tuple3<Integer, Integer, Integer>(t.f0, t.f9, t.f2);
    }
}
