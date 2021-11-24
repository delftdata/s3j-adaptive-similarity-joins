package Statistics.Latency;

import CustomDataTypes.FinalOutput;
import org.apache.flink.api.common.functions.MapFunction;

public class LatencyMeasure implements MapFunction<FinalOutput, Long> {
    @Override
    public Long map(FinalOutput o) throws Exception {
        Long start_time;
        Long end_time = System.currentTimeMillis();
        if(o.f1.f7 > o.f2.f7){
            start_time = o.f1.f7;
        }
        else{
            start_time = o.f2.f7;
        }
        return end_time - start_time;
    }
}
