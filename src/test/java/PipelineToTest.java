import CustomDataTypes.FinalOutput;
import CustomDataTypes.FinalTuple;
import CustomDataTypes.InputTuple;
import CustomDataTypes.SPTuple;
import Operators.AdaptivePartitioner.AdapativePartitioner;
import Operators.AdaptivePartitioner.AdaptivePartitionerCompanion;
import Operators.PhysicalPartitioner;
import Operators.SimilarityJoin;
import Utils.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
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

        final OutputTag<Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> sideLCentroids =
                new OutputTag<Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>>("logicalCentroids"){};

        final OutputTag<Tuple3<Long, Integer, Integer>> sideLP =
                new OutputTag<Tuple3<Long, Integer, Integer>>("logicalPartitions"){};

        OutputTag<Tuple3<Long, Integer, Integer>> sideJoins =
                new OutputTag<Tuple3<Long, Integer, Integer>>("sideJoins"){};


        DataStream<InputTuple> data = streamFactory.create2DArrayStream(inputFileName);
        env.setParallelism(givenParallelism);
        double dist_threshold = 0.5;

        DataStream<SPTuple> ppData = data.flatMap(new PhysicalPartitioner(dist_threshold, SimilarityJoinsUtil.RandomCentroids(givenParallelism, 2),(env.getMaxParallelism()/env.getParallelism())+1));

//        ppData.writeAsText(pwd+"/src/main/outputs/testfiles", FileSystem.WriteMode.OVERWRITE);
        AdaptivePartitionerCompanion adaptivePartitionerCompanion = new AdaptivePartitionerCompanion(dist_threshold, (env.getMaxParallelism()/env.getParallelism())+1);
//        adaptivePartitionerCompanion.setSideLPartitions(sideLP);
//        adaptivePartitionerCompanion.setSideLCentroids(sideLCentroids);
        DataStream<FinalTuple> partitionedData = ppData
                .keyBy(t-> t.f0)
                .process(new AdapativePartitioner(adaptivePartitionerCompanion));

        partitionedData
                .keyBy(new LogicalKeySelector())
                .flatMap(new SimilarityJoin(dist_threshold))
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
