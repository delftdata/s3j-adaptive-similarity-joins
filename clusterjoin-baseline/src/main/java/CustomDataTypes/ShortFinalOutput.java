package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple8;

public class ShortFinalOutput extends Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Boolean> {

    public ShortFinalOutput(){}

    public ShortFinalOutput(
            Long outputProcessingTime,
            Long inputProcessingTime,
            Integer left_id,
            Integer right_id,
            Integer machine_id,
            Integer partition_id,
            Integer group_id,
            Boolean comparisonResult
            ){
        super(outputProcessingTime, inputProcessingTime, left_id, right_id, machine_id, partition_id, group_id, comparisonResult);
    }

}
