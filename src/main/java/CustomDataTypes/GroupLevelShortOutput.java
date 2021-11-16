package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple4;

public class GroupLevelShortOutput extends Tuple4<Long, Integer, Integer, Long> {

    public GroupLevelShortOutput(){}

    public GroupLevelShortOutput(Long stamp,
                                 Integer machine_id,
                                 Integer group_id,
                                 Long count
                                 ){
        super(stamp, machine_id, group_id, count);
    }

}
