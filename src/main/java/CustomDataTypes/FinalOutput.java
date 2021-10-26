package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple4;

public class FinalOutput extends Tuple4<Boolean, FinalTuple, FinalTuple, Long> {

    public FinalOutput(){}

    public FinalOutput(Boolean bool,
                       FinalTuple t1,
                       FinalTuple t2,
                       Long tmsp
                       ){
        super(bool, t1, t2, tmsp);
    }
}
