package Utils;

import CustomDataTypes.FinalOutput;
import CustomDataTypes.ShortFinalOutput;
import org.apache.flink.api.common.functions.MapFunction;

public class ShortFinalOutputMapper implements MapFunction<FinalOutput, ShortFinalOutput> {
    @Override
    public ShortFinalOutput map(FinalOutput finalOutput) throws Exception {
        Long inputTimestamp;
        if(finalOutput.f1.f7 > finalOutput.f2.f7){
            inputTimestamp = finalOutput.f1.f7;
        }
        else {
            inputTimestamp = finalOutput.f2.f7;
        }

        return new ShortFinalOutput(
                finalOutput.f3,
                inputTimestamp,
                finalOutput.f1.f8,
                finalOutput.f2.f8,
                finalOutput.f1.f10,
                finalOutput.f1.f2,
                finalOutput.f1.f0,
                finalOutput.f0
                );
    }
}
