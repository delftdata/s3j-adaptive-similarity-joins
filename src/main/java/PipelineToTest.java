import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.util.OutputTag;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
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

        final OutputTag<Tuple3<Boolean, Tuple9<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[]>, Tuple9<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[]>>> sideStats =
                new OutputTag<Tuple3<Boolean, Tuple9<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[]>, Tuple9<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[]>>>("stats"){};

        DataStream<Tuple3<Long, Integer, Double[]>> data = streamFactory.create2DArrayStream(inputFileName);

        DataStream<Tuple6<Integer,String,Integer,Long,Integer,Double[]>> ppData = data.flatMap(new PhysicalPartitioner(0.05, SimilarityJoinsUtil.RandomCentroids(givenParallelism, 2),(env.getMaxParallelism()/env.getParallelism())+1));

        ppData.writeAsText(pwd+"/src/main/outputs/testfiles", FileSystem.WriteMode.OVERWRITE);

        DataStream<Tuple9<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[]>> partitionedData = ppData
                .keyBy(t-> t.f0)
                .flatMap(new AdaptivePartitioner(0.05, (env.getMaxParallelism()/env.getParallelism())+1, LOG));

        partitionedData
                .keyBy(new onlinePartitioningForSsj.LogicalKeySelector())
                .window(GlobalWindows.create())
                .trigger(new onlinePartitioningForSsj.CustomOnElementTrigger())
                .process(new SimilarityJoin(0.05, LOG))
                .process(new onlinePartitioningForSsj.CustomFiltering(sideStats))
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

    private static class Map2ID implements MapFunction<Tuple3<Boolean, Tuple9<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[]>,Tuple9<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[]>>, Tuple2<Integer,Integer>> {

        @Override
        public Tuple2<Integer, Integer> map(Tuple3<Boolean, Tuple9<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[]>, Tuple9<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[]>> t) throws Exception {
            return new Tuple2<>(t.f1.f7, t.f2.f7);
        }
    }

}
