package SelfJoin;

import CustomDataTypes.FinalOutput;
import CustomDataTypes.FinalTuple;
import CustomDataTypes.InputTuple;
import CustomDataTypes.SPTuple;
import Operators.AdaptivePartitioner.AdaptivePartitioner;
import Operators.AdaptivePartitioner.AdaptivePartitionerCompanion;
import Operators.PassthroughProcess;
import Operators.PhysicalPartitioner;
import Operators.SimilarityJoin;
import Operators.SimilarityJoinSelf;
import Utils.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;


public class PipelineToTest {

    static String pwd = Paths.get("").toAbsolutePath().toString();

    public List<Tuple2<Integer,Integer>> run(int givenParallelism, String inputFileName, Logger LOG) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamFactory streamFactory = new StreamFactory(env);
        env.setMaxParallelism(128);
        env.setParallelism(givenParallelism);

        CollectSink.values.clear();

        final OutputTag<Tuple4<Long, Boolean, FinalTuple, FinalTuple>> sideStats =
                new OutputTag<Tuple4<Long, Boolean, FinalTuple, FinalTuple>>("stats"){};

        DataStream<InputTuple> data = streamFactory.create2DArrayStream(inputFileName);
        double dist_threshold = 0.05;

        DataStream<Integer> controlStream = env.addSource(new WindowController(30, true));

        MapStateDescriptor<Void, Integer> controlStateDescriptor = new MapStateDescriptor<Void, Integer>(
                "ControlBroadcastState",
                BasicTypeInfo.VOID_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO);

// broadcast the rules and create the broadcast state
        BroadcastStream<Integer> controlBroadcastStream = controlStream
                .broadcast(controlStateDescriptor);

        DataStream<SPTuple> ppData = data.flatMap(new PhysicalPartitioner(dist_threshold, SimilarityJoinsUtil.RandomCentroids(givenParallelism, 2),(env.getMaxParallelism()/env.getParallelism())+1));

//        ppData.writeAsText(pwd+"/src/main/outputs/testfiles", FileSystem.WriteMode.OVERWRITE);
        AdaptivePartitionerCompanion adaptivePartitionerCompanion = new AdaptivePartitionerCompanion(dist_threshold, (env.getMaxParallelism()/env.getParallelism())+1);

        DataStream<FinalTuple> partitionedData = ppData
                .keyBy(t-> t.f0)
                .process(new PassthroughProcess())
                .keyBy(t -> t.f0)
                .connect(controlBroadcastStream)
                .process(new AdaptivePartitioner(adaptivePartitionerCompanion));

        partitionedData
                .keyBy(new LogicalKeySelector())
                .connect(controlBroadcastStream)
                .process(new SimilarityJoinSelf(dist_threshold))
                .process(new CustomFiltering(sideStats))
                .map(new Map2ID())
                .addSink(new CollectSink());
        env.execute();

        return CollectSink.values;
    }

    public static class CollectSink implements SinkFunction<Tuple2<Integer,Integer>> {

        // must be static
        public static final List<Tuple2<Integer,Integer>> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Tuple2<Integer,Integer> value) throws Exception {
            values.add(value);
        }
    }

    private static class Map2ID implements MapFunction<FinalOutput, Tuple2<Integer,Integer>> {

        @Override
        public Tuple2<Integer, Integer> map(FinalOutput t) throws Exception {
            return new Tuple2<>(t.f1.f8, t.f2.f8);
        }
    }

}
