package Utils;

import CustomDataTypes.FinalOutput;
import CustomDataTypes.FinalTuple;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class CustomFiltering extends ProcessFunction<
        FinalOutput,
        FinalOutput> {

    OutputTag<Tuple4<Long, Boolean, FinalTuple, FinalTuple>> sideStats;

    public CustomFiltering(
            OutputTag<Tuple4<Long, Boolean, FinalTuple, FinalTuple>> sideStats){
        this.sideStats = sideStats;
    }

    @Override
    public void processElement(FinalOutput t,
                               Context context, Collector<FinalOutput> collector)
            throws Exception {

        if(t.f0){
            collector.collect(t);
        }
        if(t.f1.f6 > t.f2.f6){
            context.output(sideStats, new Tuple4<>(t.f1.f6, t.f0, t.f1, t.f2));
        }
        else{
            context.output(sideStats, new Tuple4<>(t.f2.f6, t.f0, t.f1, t.f2));
        }
    }
}
