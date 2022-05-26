package Generators;

import CustomDataTypes.MinioConfiguration;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.Buffer;

public class IMDBStreamGenerator implements SourceFunction<Tuple3<Long, Integer, String>>, CheckpointedFunction {

    protected Integer id = 0;
    protected Long timestamp = 0L;
    protected Tuple3<Long, Integer, String> tuple3;
    protected final int rate;
    protected final Long tmsp;
    protected int tRate;
    protected int delay;
    protected volatile boolean isRunning = true;
    protected transient ListState<Tuple3<Long, Integer, String>> checkpointedTuples;
    protected int sleepInterval;
    protected String datasetFile;
    protected MinioConfiguration minio;

    public IMDBStreamGenerator(String imdbFile, int rate, Long tmsp, int delay, MinioConfiguration minio){
        this.rate = rate;
        this.tRate = rate;
        this.tmsp = tmsp;
        this.delay = 1_000_000*delay;
        this.sleepInterval = this.delay/this.rate;
        this.minio = minio;
        this.datasetFile = imdbFile;
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

    public static void busyWaitMicros(long micros){
        long waitUntil = System.nanoTime() + (micros * 1_000);
        while(waitUntil > System.nanoTime()){
            ;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void run(SourceContext<Tuple3<Long, Integer, String>> ctx) throws Exception {

        MinioClient minioClient =
                MinioClient.builder()
                        .endpoint(minio.getEndpoint())
                        .credentials(minio.getAccessKey(), minio.getSecretKey())
                        .build();
        try {
            InputStream datasetStream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket("embeddings")
                            .object(datasetFile)
                            .build());
            BufferedReader br = new BufferedReader( new InputStreamReader( datasetStream ) );

            while (isRunning && timestamp < tmsp) {

                // this synchronized block ensures that state checkpointing,
                // internal state updates and emission of elements are an atomic operation
                synchronized (ctx.getCheckpointLock()) {
                    if(tRate > 0) {
                        String title = br.readLine();
                        if (title != null){
                            ctx.collect(new Tuple3<>(timestamp, id, title));
                            id++;
                            tRate--;
                        }
                        else{
                            break;
                        }

                    }
                    else{
                        timestamp++;
                        tRate = rate;
                    }
                }
                busyWaitMicros(this.sleepInterval);
            }
            datasetStream.close();
        }
        catch (Exception e){
            e.printStackTrace();
            System.exit(-1);
        }
    }

}
