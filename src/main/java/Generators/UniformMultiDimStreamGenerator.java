package Generators;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.concurrent.TimeUnit;

public class UniformMultiDimStreamGenerator extends Uniform2DStreamGenerator{

    protected int dimensions;

    public UniformMultiDimStreamGenerator(int seed, int rate, Long tmsp, int delay, int dimensions) {
        super(seed, rate, tmsp, delay);
        this.dimensions = dimensions;
    }

    @Override
    public void run(SourceContext<Tuple3<Long, Integer, Double[]>> ctx) throws Exception{
        while (isRunning && (timestamp < tmsp)) {
            // this synchronized block ensures that state checkpointing,
            // internal state updates and emission of elements are an atomic operation
            synchronized (ctx.getCheckpointLock()) {
                if(tRate > 0) {
                    Double[] nextStreamItem = new Double[dimensions];
                    for(int d=0; d<dimensions; d++) {
                        nextStreamItem[d] = rng.nextDouble() * 2 - 1;
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
            busyWaitMicros(this.sleepInterval);
        }
    }
}
