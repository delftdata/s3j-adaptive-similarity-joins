package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;

public class SPTuple extends Tuple7<Integer,String,Integer,Long, Long,Integer,Double[]> {

    public SPTuple(){}

    public SPTuple(Integer t0, String t1, Integer t2, Long t3, Long t4, Integer t5, Double[] t6){
        super(t0, t1, t2, t3, t4, t5, t6);
    }

}
