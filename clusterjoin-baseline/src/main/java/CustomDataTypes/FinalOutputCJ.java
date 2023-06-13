package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple4;

public class FinalOutputCJ extends Tuple4<Boolean, FinalTupleCJ, FinalTupleCJ, Long> {

    public FinalOutputCJ(){}

    public FinalOutputCJ(Boolean bool,
                       FinalTupleCJ t1,
                       FinalTupleCJ t2,
                       Long tmsp
    ){
        super(bool, t1, t2, tmsp);
    }

}
