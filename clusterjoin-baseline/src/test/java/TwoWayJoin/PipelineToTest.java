package TwoWayJoin;

import CustomDataTypes.*;
import Operators.PassthroughCoProcess;
import Operators.PhysicalPartitioner;
import Operators.SimilarityJoinCJ;
import Utils.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.OutputTag;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;


public class PipelineToTest {

    static String pwd = Paths.get("").toAbsolutePath().toString();

    public List<Tuple2<String,String>> run(int givenParallelism, String stream1, String stream2, Logger LOG) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamFactory streamFactory = new StreamFactory(env);
        env.setMaxParallelism(128);
        env.setParallelism(givenParallelism);

        CollectSink.values.clear();

        final OutputTag<Tuple4<Long, Boolean, FinalTupleCJ, FinalTupleCJ>> sideStats =
                new OutputTag<Tuple4<Long, Boolean, FinalTupleCJ, FinalTupleCJ>>("stats"){};

        DataStream<InputTuple> dataStream1 = streamFactory.create2DArrayStream(stream1);
        DataStream<InputTuple> dataStream2 = streamFactory.create2DArrayStream(stream2);
        double dist_threshold = 0.1;

        HashMap<Integer, Double[]> centroids = SimilarityJoinsUtil.RandomCentroids(givenParallelism, 2);

        DataStream<SPTuple> ppData1 = dataStream1.flatMap(new PhysicalPartitioner(dist_threshold, centroids,(env.getMaxParallelism()/env.getParallelism())+1));
        DataStream<SPTuple> ppData2 = dataStream2.flatMap(new PhysicalPartitioner(dist_threshold, centroids,(env.getMaxParallelism()/env.getParallelism())+1));

//        ppData.writeAsText(pwd+"/src/main/outputs/testfiles", FileSystem.WriteMode.OVERWRITE);

        DataStream<FinalTupleCJ> partitionedData = ppData1
                .keyBy(t -> t.f0)
                .connect(ppData2.keyBy(t -> t.f0))
                .process(new PassthroughCoProcess());

        DataStream<Integer> controlStream = env.addSource(new WindowController(30, true));

        MapStateDescriptor<Void, Integer> controlStateDescriptor = new MapStateDescriptor<Void, Integer>(
                "ControlBroadcastState",
                BasicTypeInfo.VOID_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO) ;

// broadcast the rules and create the broadcast state
        BroadcastStream<Integer> controlBroadcastStream = controlStream
                .broadcast(controlStateDescriptor);

        partitionedData.keyBy(t -> t.f0)
                .connect(controlBroadcastStream)
                .process(new SimilarityJoinCJ(dist_threshold))
                .process(new CustomFiltering(sideStats))
                .map(new Map2ID())
                .addSink(new CollectSink());

        env.execute();

        return CollectSink.values;
    }

    private static class CollectSink implements SinkFunction<Tuple2<String,String>> {

        // must be static
        public static final List<Tuple2<String,String>> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Tuple2<String,String> value) throws Exception {
            values.add(value);
        }
    }

    private static class Map2ID implements MapFunction<FinalOutputCJ, Tuple2<String,String>> {

        @Override
        public Tuple2<String, String> map(FinalOutputCJ t) throws Exception {
            return new Tuple2<>(t.f1.f5.toString() + "L", t.f2.f5.toString() + "R");
        }
    }

}
