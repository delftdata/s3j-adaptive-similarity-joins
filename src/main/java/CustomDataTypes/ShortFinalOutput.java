package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple5;

public class ShortFinalOutput extends Tuple5<Long, Long, Integer, Integer, Integer> {

    public ShortFinalOutput(){}

    public ShortFinalOutput(
            Long outputProcessingTime,
            Long inputProcessingTime,
            Integer left_id,
            Integer right_id,
            Integer machine_id
            ){
        super(outputProcessingTime, inputProcessingTime, left_id, right_id, machine_id);
    }

}
