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

    private int id = 0;
    private Long timestamp = 0L;
    private Tuple3<Long, Integer, Double[]> tuple3;
    private Random rng;
    private int rate;
    private Long tmsp;
    private int tRate;
    private volatile boolean isRunning = true;
    private transient ListState<Tuple3<Long, Integer, Double[]>> checkpointedTuples;

    public Uniform2DStreamGenerator(int seed, int rate, Long tmsp){
        this.tRate = rate;
        this.rate = rate;
        this.tmsp = tmsp;
        this.rng = new Random(seed);

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
            synchronized (ctx.getCheckpointLock()) {
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
            }
            if(tRate == rate) {
                TimeUnit.SECONDS.sleep(5);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
