package Statistics.GroupLevelFinalComputations;

import CustomDataTypes.GroupLevelShortOutput;
import CustomDataTypes.ShortOutput;
import org.apache.flink.api.common.functions.ReduceFunction;

public class GroupLevelFinalComputationsReduce implements ReduceFunction<GroupLevelShortOutput> {

    @Override
    public GroupLevelShortOutput reduce(GroupLevelShortOutput t1, GroupLevelShortOutput t2) throws Exception {
        return new GroupLevelShortOutput(t1.f0, t1.f1, t1.f2, t1.f3, t1.f4,t1.f5+t2.f5);
        // t1.f4 added just to match the new fields of GroupLevelShortOutput
    }
}
