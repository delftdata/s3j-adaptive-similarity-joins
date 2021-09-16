package Generators;

import org.apache.commons.math3.distribution.ParetoDistribution;
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

public class Pareto2DStreamGenerator implements SourceFunction<Tuple3<Long, Integer, Double[]>>, CheckpointedFunction {

    private int id = 0;
    private Long timestamp;
    private Tuple3<Long, Integer, Double[]> tuple3;
    private final ParetoDistribution pareto;
    private int rate;
    private Long tmsp;
    private int tRate;
    private volatile boolean isRunning = true;
    private transient ListState<Tuple3<Long, Integer, Double[]>> checkpointedTuples;

    public Pareto2DStreamGenerator(Double scale, Double shape, int rate, Long tmsp){
        this.pareto =new ParetoDistribution(new Well19937c(42), scale, shape);
        this.tRate = rate;
        this.rate = rate;
        this.tmsp = tmsp;
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
    public void run(SourceContext<Tuple3<Long, Integer, Double[]>> ctx) throws Exception {
        while (isRunning && timestamp < tmsp) {
            // this synchronized block ensures that state checkpointing,
            // internal state updates and emission of elements are an atomic operation
            synchronized (ctx.getCheckpointLock()) {
                if(tRate > 0) {
                    Double[] nextStreamItem = new Double[2];
                    nextStreamItem[0] = pareto.sample();
                    nextStreamItem[1] = pareto.sample();
                    ctx.collect(new Tuple3<>(timestamp, id, nextStreamItem));
                    id++;
                    tRate--;
                }
                else{
                    timestamp++;
                    tRate = rate;
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
