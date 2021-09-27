import CustomDataTypes.FinalOutput;
import CustomDataTypes.FinalTuple;
import CustomDataTypes.SPTuple;
import Operators.AdaptivePartitioner;
import Operators.PhysicalPartitioner;
import Operators.SimilarityJoin;
import Utils.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class onlinePartitioningForSsj {

    private static final Logger LOG = LoggerFactory.getLogger(onlinePartitioningForSsj.class);

    static String pwd = Paths.get("").toAbsolutePath().toString();


    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamFactory streamFactory = new StreamFactory(env);
        env.setMaxParallelism(128);
        env.setParallelism(10);

        LOG.info("Enter main.");

        final OutputTag<Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>> sideLCentroids =
                new OutputTag<Tuple2<Integer,HashMap<Integer, Tuple3<Long, Integer, Double[]>>>>("logicalCentroids"){};

        final OutputTag<Tuple3<Long, Integer, Integer>> sideLP =
                new OutputTag<Tuple3<Long, Integer, Integer>>("logicalPartitions"){};

        final OutputTag<Tuple3<Long, Integer, Integer>> sideJoins =
                new OutputTag<Tuple3<Long, Integer, Integer>>("joinComputations"){};

        env.setParallelism(1);

        String pathToFile = args[0];

        DataStream<Tuple3<Long, Integer, Double[]>> data;
        int centroidsDim = 2;
        int centroidsNum = 10;

        if (pathToFile.equals("gaussian_2D_generator")){
            data = streamFactory.createGaussian2DStream(42, 1000, 10L);
        }
        else if(pathToFile.equals("skewed_gaussian_2D_generator")){
            data = streamFactory.createSkewedGaussian2DStream(42, 1000, 10L);
        }
        else if(pathToFile.equals("uniform_2D_generator")){
            data = streamFactory.createUniform2DStream(42, 1000, 10L);
        }
        else if(pathToFile.equals("pareto_2D_generator")){
            data = streamFactory.createPareto2DStream(1.0, 10.0 , 1000, 10L);
        }
        else if(pathToFile.equals("zipfian_word_generator")){
            data = streamFactory.createZipfianWordStream("wiki-news-300d-1K.vec", 2.0, 1000, 10L)
                    .map(new WordsToEmbeddingMapper("wiki-news-300d-1K.vec"));
            centroidsDim = 300;
        }
        else{
            data = streamFactory.create2DArrayStream(pathToFile);
        }


        data.writeAsText(pwd+"/src/main/resources/streamed_dataset.txt", FileSystem.WriteMode.OVERWRITE);

        env.setParallelism(10);

        DataStream<SPTuple> ppData = data.
                flatMap(new PhysicalPartitioner(0.1, SimilarityJoinsUtil.RandomCentroids(centroidsNum, centroidsDim), (env.getMaxParallelism()/env.getParallelism())+1));


        SingleOutputStreamOperator<FinalTuple> lpData = ppData
                .keyBy(t -> t.f0)
                .process(new AdaptivePartitioner(0.1, (env.getMaxParallelism()/env.getParallelism())+1, LOG, sideLP, sideLCentroids));

        final OutputTag<Tuple4<Long, Boolean, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>>> sideStats =
                new OutputTag<Tuple4<Long, Boolean, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>>>("stats"){};

        SingleOutputStreamOperator<FinalOutput>
                unfilteredSelfJoinedStream = lpData
                .keyBy(new LogicalKeySelector())
                .window(GlobalWindows.create())
                .trigger(new CustomOnElementTrigger())
                .process(new SimilarityJoin(0.1, LOG, sideJoins));

        SingleOutputStreamOperator<FinalOutput>
                selfJoinedStream = unfilteredSelfJoinedStream
                .process(new CustomFiltering(sideStats));


        LOG.info(env.getExecutionPlan());

        JobExecutionResult result = env.execute("ssj");
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute");

    }
}

// TODO:
//  - Use embeddings from end-to-end.  DONE
//  - Add latency metric.
//  - Create Tests.       DONE
