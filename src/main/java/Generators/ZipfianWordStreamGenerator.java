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

public class ZipfianWordStreamGenerator implements SourceFunction<Tuple3<Long, Integer, String>>, CheckpointedFunction {

    private Integer id = 0;
    private Long timestamp = 0L;
    private Tuple3<Long, Integer, String> tuple3;
    private final String[] wordArray;
    private final ZipfDistribution zipf;
    private final int rate;
    private final Long tmsp;
    private int tRate;
    private int delay;
    private volatile boolean isRunning = true;
    private transient ListState<Tuple3<Long, Integer, String>> checkpointedTuples;

    public ZipfianWordStreamGenerator(String[] words, Double zipfExp, int rate, Long tmsp, int delay){
        this.wordArray = words;
        this.zipf =new ZipfDistribution(new Well19937c(42), wordArray.length, zipfExp);
        this.tRate = rate;
        this.rate = rate;
        this.tmsp = tmsp;
        this.delay = delay;
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
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
