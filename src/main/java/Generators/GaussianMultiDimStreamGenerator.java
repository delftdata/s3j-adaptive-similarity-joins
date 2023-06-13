package Generators;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

public class GaussianMultiDimStreamGenerator extends Gaussian2DStreamGenerator{

    private int dimensions;

    public GaussianMultiDimStreamGenerator(int seed, int rate, Long tmsp, int delay, int dimensions) {
        super(seed, rate, tmsp, delay);
        this.dimensions = dimensions;
    }

    @Override
    public void run(SourceFunction.SourceContext<Tuple3<Long, Integer, Double[]>> ctx) throws Exception {
        while (isRunning && (timestamp < tmsp)) {
            // this synchronized block ensures that state checkpointing,
            // internal state updates and emission of elements are an atomic operation
            long startMeasuring = System.nanoTime();
            synchronized (ctx.getCheckpointLock()) {
                if(tRate > 0) {
                    Double[] nextStreamItem = new Double[dimensions];
                    for(int d=0; d<dimensions; d++) {
                        nextStreamItem[d] = rng.nextGaussian() * 2 - 1;
                    }
                    ctx.collect(new Tuple3<>(timestamp, id, nextStreamItem));
                    id++;
                    tRate--;
                }
                else{
                    timestamp++;
                    tRate = rate;
                }
            }
            busyWaitMicros(this.sleepInterval, startMeasuring);
        }
    }
}
