package Utils;

import CustomDataTypes.InputTuple;
import org.apache.flink.api.common.functions.MapFunction;

public class IngestTimeMapper implements MapFunction<InputTuple, InputTuple>{
    @Override
    public InputTuple map(InputTuple inputTuple) throws Exception {
        inputTuple.f1 = System.currentTimeMillis();
        return inputTuple;
    }
}
