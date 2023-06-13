package Utils;

import CustomDataTypes.FinalOutput;
import CustomDataTypes.FinalOutputCJ;
import CustomDataTypes.FinalTuple;
import CustomDataTypes.FinalTupleCJ;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class CustomFiltering extends ProcessFunction<
        FinalOutputCJ,
        FinalOutputCJ> {

    OutputTag<Tuple4<Long, Boolean, FinalTupleCJ, FinalTupleCJ>> sideStats;

    public CustomFiltering(
            OutputTag<Tuple4<Long, Boolean, FinalTupleCJ, FinalTupleCJ>> sideStats){
        this.sideStats = sideStats;
    }

    @Override
    public void processElement(FinalOutputCJ t,
                               Context context, Collector<FinalOutputCJ> collector)
            throws Exception {

        if(t.f0){
            collector.collect(t);
        }
        if(t.f1.f4 > t.f2.f4){
            context.output(sideStats, new Tuple4<>(t.f1.f4, t.f0, t.f1, t.f2));
        }
        else{
            context.output(sideStats, new Tuple4<>(t.f2.f4, t.f0, t.f1, t.f2));
        }
    }
}
