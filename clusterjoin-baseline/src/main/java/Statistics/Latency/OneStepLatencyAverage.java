package Statistics.Latency;

import CustomDataTypes.FinalOutput;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.concurrent.TimeUnit;

public class OneStepLatencyAverage implements MapFunction<FinalOutput, Long> {

    Long sum = 0L;
    Long count = 0L;

    @Override
    public Long map(FinalOutput t) throws Exception {
        Long start_time, latency;
        Long end_time = System.currentTimeMillis();
        if(t.f1.f7 > t.f2.f7){
            start_time = t.f1.f7;
        }
        else{
            start_time = t.f2.f7;
        }
        latency = end_time - start_time;

        sum += latency;
        count++;

        return TimeUnit.MILLISECONDS.toSeconds(sum/count);

    }
}
