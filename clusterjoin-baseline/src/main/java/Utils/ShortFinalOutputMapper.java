package Utils;

import CustomDataTypes.FinalOutput;
import CustomDataTypes.FinalOutputCJ;
import CustomDataTypes.ShortFinalOutput;
import CustomDataTypes.ShortFinalOutputCJ;
import org.apache.flink.api.common.functions.MapFunction;

public class ShortFinalOutputMapper implements MapFunction<FinalOutputCJ, ShortFinalOutputCJ> {
    @Override
    public ShortFinalOutputCJ map(FinalOutputCJ finalOutput) throws Exception {
        Long inputTimestamp;
        if(finalOutput.f1.f4 > finalOutput.f2.f4){
            inputTimestamp = finalOutput.f1.f4;
        }
        else {
            inputTimestamp = finalOutput.f2.f4;
        }

        return new ShortFinalOutputCJ(
                finalOutput.f3,
                inputTimestamp,
                finalOutput.f1.f5,
                finalOutput.f2.f5,
                finalOutput.f1.f2,
                finalOutput.f0
                );
    }
}
