package CustomDataTypes;


import org.apache.flink.api.java.tuple.Tuple6;

public class ShortFinalOutputCJ extends Tuple6<Long,Long,Integer,Integer,Integer,Boolean> {

    public ShortFinalOutputCJ(){}

    public ShortFinalOutputCJ(
            Long outputProcessingTime,
            Long inputProcessingTime,
            Integer left_id,
            Integer right_id,
            Integer partition_id,
            Boolean comparisonResult
    ){
        super(outputProcessingTime, inputProcessingTime, left_id, right_id, partition_id, comparisonResult);
    }

}
