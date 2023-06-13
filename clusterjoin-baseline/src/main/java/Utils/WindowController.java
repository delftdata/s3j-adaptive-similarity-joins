package Utils;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.time.LocalTime;

public class WindowController implements SourceFunction<Integer>, CheckpointedFunction {

    protected volatile boolean isRunning = true;
    private int windowLength = 60;
    private boolean test;

    public WindowController(){}

    public WindowController(int windowLength){
        this.windowLength = windowLength;
        this.test = false;
    }

    public WindowController(int windowLength, boolean test){
        this.windowLength = windowLength;
        this.test = test;
    }



    // Thread sleep from https://stackoverflow.com/questions/63138813/not-able-to-sleep-in-custom-source-funtion-in-apache-flink-which-is-union-with-o
    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (isRunning) {
            LocalTime end = LocalTime.now().plusSeconds(this.windowLength);
            while (LocalTime.now().compareTo(end) < 0) {
                try {
                    Thread.sleep(Duration.between(LocalTime.now(), end).toMillis());
                } catch (InterruptedException e) {
                    // swallow interruption unless source is canceled
                    if (!isRunning) {
                        Thread.interrupted();
                        return;
                    }
                }
            }
            ctx.collect(1);
            if(test){
                isRunning = false;
            }

        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
