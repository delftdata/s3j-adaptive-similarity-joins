import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;
import scala.concurrent.java8.FuturesConvertersImpl;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GlobalWindowCorrectnessTest {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalWindowCorrectnessTest.class);

    static String pwd = Paths.get("").toAbsolutePath().toString();

    @Test
    public void testJoinResults() throws Exception{

        PipelineToTest pipeline = new PipelineToTest();
        List<Tuple2<Integer,Integer>> results = pipeline.run(10, "wordStream.txt");

//        System.out.println(CollectSink.values.toString());
//        System.out.println(getGroundTruth("wordStreamGroundTruth.txt"));
        for(Tuple2<Integer,Integer> v : getGroundTruth("wordStreamGroundTruth.txt")){
            boolean cont = results.contains(v);
            if(!cont) {
                System.out.format("(%d,%d): %b\n", v.f0, v.f1, cont);
            }
        }
        assertTrue(results.containsAll(getGroundTruth("wordStreamGroundTruth.txt")));
        assertTrue(getGroundTruth("wordStreamGroundTruth.txt").containsAll(results));

    }

    private static ArrayList<Tuple2<Integer,Integer>> getGroundTruth(String filename) throws Exception{
        ArrayList<Tuple2<Integer,Integer>> groudTruth = new ArrayList<>();

        try (Stream<String> lines = Files.lines(Paths.get(pwd + "/src/main/resources/"+ filename), Charset.defaultCharset())) {
            lines.map(l -> l.split(","))
                    .forEach(l -> groudTruth.add(new Tuple2<Integer,Integer>(Integer.parseInt(l[0]),Integer.parseInt(l[1]))));
        }

        return groudTruth;
    }

}
