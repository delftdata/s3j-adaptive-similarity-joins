package Statistics.Latency;

import CustomDataTypes.FinalOutput;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class BernoulliSampling extends ProcessFunction<FinalOutput, FinalOutput> {

    private BinomialDistribution bernoulli;

    public BernoulliSampling(double probability){
        this.bernoulli = new BinomialDistribution(1,probability);
    }

    @Override
    public void processElement(FinalOutput t, Context context, Collector<FinalOutput> collector) throws Exception {
        int decision = bernoulli.sample();
        if(decision == 1){
            collector.collect(t);
        }
    }
}
