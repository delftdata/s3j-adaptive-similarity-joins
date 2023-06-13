package Generators;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class UniformWordStreamGenerator extends ZipfianWordStreamGenerator{

    private Random rng;
    public UniformWordStreamGenerator(String[] words, int seed, int rate, Long tmsp, int delay) {
        super(words, 1.0, rate, tmsp, delay);
        this.rng = new Random(seed);
    }

    public void run(SourceContext<Tuple3<Long, Integer, String>> ctx) throws Exception {
        while (isRunning && timestamp < tmsp) {
            // this synchronized block ensures that state checkpointing,
            // internal state updates and emission of elements are an atomic operation
            long startMeasuring = System.nanoTime();
            synchronized (ctx.getCheckpointLock()) {
                if(tRate > 0) {
                    int wordIndex = rng.nextInt(wordArray.length);
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
}
