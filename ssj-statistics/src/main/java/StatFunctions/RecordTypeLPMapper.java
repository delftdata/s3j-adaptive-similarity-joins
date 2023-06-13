package StatFunctions;

import CustomDataTypes.GroupLevelShortOutput;
import StatFunctions.GroupLevelFinalComputations.GroupLevelCombineProcessFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.HashMap;

public class RecordTypeLPMapper implements MapFunction<GroupLevelShortOutput, GroupLevelShortOutput> {

    private HashMap<Tuple4<Integer,Integer,Integer,String>, Tuple2<Long,Long>> stats = new HashMap<>();

    @Override
    public GroupLevelShortOutput map(GroupLevelShortOutput t) throws Exception {
        Tuple4<Integer,Integer,Integer,String> key = new Tuple4<>(t.f1,t.f2,t.f3,t.f4);
        if (!stats.containsKey(key)){
            stats.put(key, new Tuple2<>(0L,0L));
        }

        Tuple2<Long, Long> countWithTime = stats.get(key);
        Long nCount = countWithTime.f1+t.f5;
        stats.put(key, new Tuple2<>(t.f0, nCount));

        return new GroupLevelShortOutput(t.f0, t.f1, t.f2, t.f3, t.f4, nCount);
    }
}