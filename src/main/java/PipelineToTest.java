import Operators.AdaptivePartitioner;
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

        final OutputTag<Tuple4<Long, Boolean, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>>> sideStats =
                new OutputTag<Tuple4<Long, Boolean, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>>>("stats"){};

        final OutputTag<Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> sideLCentroids =
                new OutputTag<Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>>("logicalCentroids"){};

        final OutputTag<Tuple3<Long, Integer, Integer>> sideLP =
                new OutputTag<Tuple3<Long, Integer, Integer>>("logicalPartitions"){};

        OutputTag<Tuple3<Long, Integer, Integer>> sideJoins =
                new OutputTag<Tuple3<Long, Integer, Integer>>("sideJoins"){};

        env.setParallelism(1);
        DataStream<Tuple3<Long, Integer, Double[]>> data = streamFactory.create2DArrayStream(inputFileName);
        env.setParallelism(givenParallelism);

        DataStream<Tuple6<Integer,String,Integer,Long,Integer,Double[]>> ppData = data.flatMap(new PhysicalPartitioner(0.05, SimilarityJoinsUtil.RandomCentroids(givenParallelism, 2),(env.getMaxParallelism()/env.getParallelism())+1));

//        ppData.writeAsText(pwd+"/src/main/outputs/testfiles", FileSystem.WriteMode.OVERWRITE);

        DataStream<Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>> partitionedData = ppData
                .keyBy(t-> t.f0)
                .process(new AdaptivePartitioner(0.05, (env.getMaxParallelism()/env.getParallelism())+1, LOG, sideLP, sideLCentroids));

        partitionedData
                .keyBy(new LogicalKeySelector())
                .window(GlobalWindows.create())
                .trigger(new CustomOnElementTrigger())
                .process(new SimilarityJoin(0.05, LOG, sideJoins))
                .process(new CustomFiltering(sideStats))
                .map(new Map2ID())
                .addSink(new CollectSink());

        env.execute();

        return CollectSink.values;
    }

    private static class CollectSink implements SinkFunction<Tuple2<Integer,Integer>> {

        // must be static
        public static final List<Tuple2<Integer,Integer>> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Tuple2<Integer,Integer> value) throws Exception {
            values.add(value);
        }
    }

    private static class Map2ID implements MapFunction<Tuple3<Boolean, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>,Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>>, Tuple2<Integer,Integer>> {

        @Override
        public Tuple2<Integer, Integer> map(Tuple3<Boolean, Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer>, Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[],Integer>> t) throws Exception {
            return new Tuple2<>(t.f1.f7, t.f2.f7);
        }
    }

}
