package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple4;

public class InputTuple extends Tuple4<Long,Long,Integer,Double[]> {

    public InputTuple(){}

    public InputTuple(Long t0, Long t1, Integer t2, Double[] t3){
        super(t0, t1, t2, t3);
    }



}