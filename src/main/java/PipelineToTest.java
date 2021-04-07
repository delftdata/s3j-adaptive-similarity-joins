import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PipelineToTest {

    public List<Tuple2<Integer,Integer>> run(int givenParallelism, String inputFileName) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamFactory streamFactory = new StreamFactory(env);
        env.setMaxParallelism(128);
        env.setParallelism(givenParallelism);

        CollectSink.values.clear();

        final OutputTag<Tuple3<Boolean, Tuple7<Integer,String,Integer,String,Long,Integer,String>, Tuple7<Integer,String,Integer,String,Long,Integer,String>>> sideStats =
                new OutputTag<Tuple3<Boolean, Tuple7<Integer,String,Integer,String,Long,Integer,String>, Tuple7<Integer,String,Integer,String,Long,Integer,String>>>("stats"){};

        DataStream<Tuple3<Long, Integer, String>> data = streamFactory.createSimpleWordsStream(inputFileName);

        DataStream<Tuple5<Integer,String,Long,Integer,String>> ppData = data.flatMap(new onlinePartitioningForSsj.PhysicalPartitioner("wiki-news-300d-1K.vec", 0.3, SimilarityJoinsUtil.RandomCentroids(10),(env.getMaxParallelism()/env.getParallelism())+1));

        DataStream<Tuple7<Integer,String,Integer,String,Long,Integer,String>> partitionedData = ppData
                .keyBy(t-> t.f0)
                .flatMap(new onlinePartitioningForSsj.AdaptivePartitioner("wiki-news-300d-1K.vec", 0.3, (env.getMaxParallelism()/env.getParallelism())+1));

        partitionedData
                .keyBy(new onlinePartitioningForSsj.LogicalKeySelector())
                .window(GlobalWindows.create())
                .trigger(new onlinePartitioningForSsj.CustomOnElementTrigger())
                .process(new onlinePartitioningForSsj.SimilarityJoin("wiki-news-300d-1K.vec", 0.3))
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

    private static class Map2ID implements MapFunction<Tuple3<Boolean, Tuple7<Integer,String,Integer,String,Long,Integer,String>,Tuple7<Integer,String,Integer,String,Long,Integer,String>>, Tuple2<Integer,Integer>> {

        @Override
        public Tuple2<Integer, Integer> map(Tuple3<Boolean, Tuple7<Integer, String, Integer, String, Long, Integer, String>, Tuple7<Integer, String, Integer, String, Long, Integer, String>> t) throws Exception {
            return new Tuple2<>(t.f1.f5, t.f2.f5);
        }
    }

}
