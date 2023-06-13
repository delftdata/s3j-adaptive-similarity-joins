package Generators;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;

public class UniformMultiDimStreamGenerator implements SourceFunction<Tuple3<Long, Integer, Double[]>> {
    private int id = 0;
    private final int dimensions;
    private final int messagesPerSecond;
    private final int sleepsPerSecond;
    private final int sleepTime;
    private final int duration;
    private boolean isRunning = true;
    private final Random rng;
    public UniformMultiDimStreamGenerator(int seed,
                                          int dimensions ,
                                          int messagesPerSecond,
                                          int sleepsPerSecond,
                                          int sleepTime,  // ms
                                          int duration) {
        this.rng = new Random(seed);
        this.dimensions = dimensions;
        this.messagesPerSecond = messagesPerSecond;
        this.sleepsPerSecond = sleepsPerSecond;
        this.sleepTime = sleepTime;
        this.duration = duration;
    }
    public void run(SourceFunction.SourceContext<Tuple3<Long, Integer, Double[]>> ctx) throws Exception{
        int interval = this.messagesPerSecond / this.sleepsPerSecond;
        for (int i = 1; i <= this.duration; i++){
            long secStart = System.currentTimeMillis();
            for (int j = 1; j <= this.messagesPerSecond; j++){
                if (j % interval == 0){ Thread.sleep(this.sleepTime); }
                Double[] nextStreamItem = new Double[this.dimensions];
                for(int d = 0; d < this.dimensions; d++) {
                    nextStreamItem[d] = this.rng.nextDouble() * 2 - 1;
                }
                ctx.collect(new Tuple3<>((long) this.id, this.id, nextStreamItem));
                this.id++;
            }
            long secEnd = System.currentTimeMillis();
            long lps = secEnd - secStart;
            if ( lps < 1 ){ Thread.sleep(1000 - lps); }
        }
    }
    @Override    public void cancel() {
        isRunning = false;
    }
}