package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple8;

public class FinalTupleCJ extends Tuple8<Integer,String,Integer,Long, Long,Integer,Double[], String> {

    public FinalTupleCJ(){}

    public FinalTupleCJ(Integer partitionID,
                        String recordType,
                        Integer machineID,
                        Long ingestionTime,
                        Long eventTime,
                        Integer recordID,
                        Double[] arrayValue,
                        String streamID
    ){
        super(partitionID, recordType, machineID, ingestionTime, eventTime, recordID, arrayValue, streamID);
    }

}
