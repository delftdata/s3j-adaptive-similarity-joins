package StatFunctions.Latency;

import CustomDataTypes.ShortFinalOutput;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class BernoulliSampling extends ProcessFunction<ShortFinalOutput, ShortFinalOutput> {

    private BinomialDistribution bernoulli;

    public BernoulliSampling(double probability){
        this.bernoulli = new BinomialDistribution(1,probability);
    }

    @Override
    public void processElement(ShortFinalOutput t, Context context, Collector<ShortFinalOutput> collector) throws Exception {
        int decision = bernoulli.sample();
        if(decision == 1){
            collector.collect(t);
        }
    }
}
