package StatFunctions.Latency;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.concurrent.TimeUnit;


public class AverageLatency implements MapFunction<Long, Long> {

    Long sum = 0L;
    Long count = 0L;

    @Override
    public Long map(Long aLong) throws Exception {

        sum = sum + aLong;
        count++;
        Long latency = sum/count;
        latency = TimeUnit.MILLISECONDS.toSeconds(latency);
        return latency;

    }
}
