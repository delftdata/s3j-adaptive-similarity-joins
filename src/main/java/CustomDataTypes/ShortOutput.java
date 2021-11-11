package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple3;

public class ShortOutput extends Tuple3<Long, Integer, Long> {

    public ShortOutput(){}

    public ShortOutput(Long stamp,
                       Integer id,
                       Long count
                       ){
        super(stamp, id, count);
    }
}
