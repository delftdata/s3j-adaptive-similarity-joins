package Utils;

import CustomDataTypes.FinalOutput;
import CustomDataTypes.ShortOutput;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class KafkaOutputReducer implements MapFunction<FinalOutput, ShortOutput> {
    @Override
    public ShortOutput map(FinalOutput finalOutput) throws Exception {
        return new ShortOutput(finalOutput.f3, finalOutput.f1.f10, 1L);
    }
}
