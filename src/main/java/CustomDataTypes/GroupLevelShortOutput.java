package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple5;

public class GroupLevelShortOutput extends Tuple5<Long, Integer, Integer, Integer, Long> {

    public GroupLevelShortOutput(){}

    public GroupLevelShortOutput(Long stamp,
                                 Integer machine_id,
                                 Integer space_id,
                                 Integer group_id,
                                 Long count
                                 ){
        super(stamp, machine_id, space_id, group_id, count);
    }

}
