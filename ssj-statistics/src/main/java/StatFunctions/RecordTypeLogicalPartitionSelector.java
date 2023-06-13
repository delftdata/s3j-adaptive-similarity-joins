package StatFunctions;

import CustomDataTypes.GroupLevelShortOutput;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;

public class RecordTypeLogicalPartitionSelector implements KeySelector<GroupLevelShortOutput,
        Tuple4<Integer, Integer, Integer, String>> {


    @Override
    public Tuple4<Integer, Integer, Integer, String> getKey(GroupLevelShortOutput t) throws Exception {
        return new Tuple4<>(t.f1, t.f2, t.f3, t.f4);
    }
}