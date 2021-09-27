package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple10;

public class FinalTuple extends Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>{

    public FinalTuple(){}

    public FinalTuple(Integer t0, String t1, Integer t2, String t3, Integer t4, Integer t5, Long t6, Integer t7, Double[] t8, Integer t9){
        super(t0, t1, t2, t3, t4, t5, t6, t7, t8, t9);
    }



}
