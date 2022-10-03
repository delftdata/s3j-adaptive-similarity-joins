package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple8;

public class JointTuple extends Tuple8<Integer,String,Integer,Long, Long,Integer,Double[],String> {

    public JointTuple(){};

    public JointTuple(SPTuple sp, String origin){
        super(sp.f0, sp.f1, sp.f2, sp.f3, sp.f4, sp.f5, sp.f6, origin);
    }

}
