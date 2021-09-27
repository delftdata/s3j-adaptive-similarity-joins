package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple6;

public class SPTuple extends Tuple6<Integer,String,Integer,Long,Integer,Double[]> {

    public SPTuple(){}

    public SPTuple(Integer t0, String t1, Integer t2, Long t3, Integer t4, Double[] t5){
        super(t0, t1, t2, t3, t4, t5);
    }

}
