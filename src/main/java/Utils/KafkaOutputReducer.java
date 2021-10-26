package Utils;

import CustomDataTypes.FinalOutput;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class KafkaOutputReducer implements MapFunction<FinalOutput, Tuple3<Long,Integer,Long>> {
    @Override
    public Tuple3<Long, Integer, Long> map(FinalOutput finalOutput) throws Exception {
        return new Tuple3<>(finalOutput.f3, finalOutput.f1.f10, 1L);
    }
}
