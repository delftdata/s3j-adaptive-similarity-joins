package Statistics;

import CustomDataTypes.FinalOutput;
import CustomDataTypes.ShortOutput;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

public class LoadBalancingStats {

    public void prepare(SingleOutputStreamOperator<FinalOutput> mainStream,
                        FlinkKafkaProducer<ShortOutput> myStatsProducer){

        //<------- comparisons by physical partition per window --------->
        OutputTag<Tuple3<Long, Integer, Long>> lateJoin = new OutputTag<Tuple3<Long, Integer, Long>>("lateJoin"){};
        SingleOutputStreamOperator<ShortOutput> check =
        mainStream
                .map(t -> new ShortOutput(t.f3, t.f1.f10, 1L))
                .returns(TypeInformation.of(ShortOutput.class))
                .keyBy(t -> t.f1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(2)
                .map(x -> new ShortOutput(System.currentTimeMillis(), x.f1, x.f2));

        check.addSink(myStatsProducer);
    }

}
