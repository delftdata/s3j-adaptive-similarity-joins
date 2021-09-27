package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple3;

public class FinalOutput extends Tuple3<Boolean, FinalTuple, FinalTuple>{

    public FinalOutput(){}

    public FinalOutput(Boolean bool,
                       FinalTuple t1,
                       FinalTuple t2
                       ){
        super(bool, t1, t2);
    }
}
