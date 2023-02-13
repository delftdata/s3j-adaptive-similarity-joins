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

import java.util.concurrent.TimeUnit;

public class ZipfianWordStreamGenerator implements SourceFunction<Tuple3<Long, Integer, String>>, CheckpointedFunction {

    protected Integer id = 0;
    protected Long timestamp = 0L;
    protected Tuple3<Long, Integer, String> tuple3;
    protected final String[] wordArray;
    protected final ZipfDistribution zipf;
    protected final int rate;
    protected final Long tmsp;
    protected int tRate;
    protected int delay;
    protected volatile boolean isRunning = true;
    protected transient ListState<Tuple3<Long, Integer, String>> checkpointedTuples;
    protected int sleepInterval;

    public ZipfianWordStreamGenerator(String[] words, Double zipfExp, int rate, Long tmsp, int delay){
        this.wordArray = words;
        this.zipf =new ZipfDistribution(new Well19937c(42), wordArray.length, zipfExp);
        this.tRate = rate;
        this.rate = rate;
        this.tmsp = tmsp;
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
                .getListState(new ListStateDescriptor<>("tuples", TypeInformation.of(new TypeHint<Tuple3<Long, Integer, String>>() {})));

    }

    @Override
    public void run(SourceContext<Tuple3<Long, Integer, String>> ctx) throws Exception {
        while (isRunning && timestamp < tmsp) {
            // this synchronized block ensures that state checkpointing,
            // internal state updates and emission of elements are an atomic operation
            long startMeasuring = System.nanoTime();
            synchronized (ctx.getCheckpointLock()) {
                if(tRate > 0) {
                    int wordIndex = zipf.sample();
                    String nextWord = wordArray[wordIndex];
                    ctx.collect(new Tuple3<>(timestamp, id, nextWord));
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
