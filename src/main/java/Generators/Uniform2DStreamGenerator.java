package Generators;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Uniform2DStreamGenerator implements SourceFunction<Tuple3<Long, Integer, Double[]>>, CheckpointedFunction {

    protected int id = 0;
    protected Long timestamp = 0L;
    protected Tuple3<Long, Integer, Double[]> tuple3;
    protected Random rng;
    protected int rate;
    protected Long tmsp;
    protected int tRate;
    protected int delay;
    protected volatile boolean isRunning = true;
    protected transient ListState<Tuple3<Long, Integer, Double[]>> checkpointedTuples;
    protected int sleepInterval;

    public Uniform2DStreamGenerator(int seed, int rate, Long tmsp, int delay){
        this.tRate = rate;
        this.rate = rate;
        this.tmsp = tmsp;
        this.rng = new Random(seed);
        this.delay = 1_000_000*delay;
        this.sleepInterval = this.delay/this.rate;
    }


    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        this.checkpointedTuples.clear();
        this.checkpointedTuples.add(tuple3);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.checkpointedTuples = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("tuples", TypeInformation.of(new TypeHint<Tuple3<Long, Integer, Double[]>>() {})));

    }

    @Override
    public void run(SourceFunction.SourceContext<Tuple3<Long, Integer, Double[]>> ctx) throws Exception {
        while (isRunning && (timestamp < tmsp)) {
            // this synchronized block ensures that state checkpointing,
            // internal state updates and emission of elements are an atomic operation
            long startMeasuring = System.nanoTime();
            if(tRate > 0) {
                Double[] nextStreamItem = new Double[2];
                nextStreamItem[0] = rng.nextDouble() * 2 - 1;
                nextStreamItem[1] = rng.nextDouble() * 2 - 1;
                ctx.collect(new Tuple3<>(timestamp, id, nextStreamItem));
                id++;
                tRate--;
            }
            else{
                timestamp++;
                tRate = rate;
            }
        
            busyWaitMicros(this.sleepInterval, startMeasuring);
        }
    }
// code from http://www.rationaljava.com/2015/10/measuring-microsecond-in-java.html
    public static void busyWaitMicros(long micros, long startMeasuring){
        long waitUntil = startMeasuring + (micros * 1_000);
        while(waitUntil > System.nanoTime()){
            ;
        }
    }
    @Override
    public void cancel() {
        isRunning = false;
    }

}
