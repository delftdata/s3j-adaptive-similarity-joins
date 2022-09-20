package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple6;

public class GroupLevelShortOutput extends Tuple6<Long, Integer, Integer, Integer, String, Long> {

    public GroupLevelShortOutput(){}

    public GroupLevelShortOutput(Long stamp,
                                 Integer machine_id,
                                 Integer space_id,
                                 Integer group_id,
                                 String type,
                                 Long count
                                 ){
        super(stamp, machine_id, space_id, group_id, type, count);
    }

}
