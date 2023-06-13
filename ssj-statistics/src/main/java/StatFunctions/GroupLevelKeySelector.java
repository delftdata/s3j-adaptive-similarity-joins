package StatFunctions;

import CustomDataTypes.GroupLevelShortOutput;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.kafka.common.KafkaException;

public class GroupLevelKeySelector implements KeySelector<
        GroupLevelShortOutput,
        Tuple3<Integer,Integer,Integer>> {
    @Override
    public Tuple3<Integer, Integer, Integer> getKey(GroupLevelShortOutput t) throws Exception {
        return new Tuple3<Integer, Integer, Integer>(t.f1, t.f2, t.f3);
    }
}
