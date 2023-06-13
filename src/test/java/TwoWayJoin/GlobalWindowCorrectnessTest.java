package TwoWayJoin;

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
        List<Tuple2<String,String>> results = pipeline.run(10 , "src/test/resources/twoWayStreamLeft.txt", "src/test/resources/twoWayStreamRight.txt", LOG);

//        System.out.println(CollectSink.values.toString());
//        System.out.println(getGroundTruth("wordStreamGroundTruth.txt"));
        ArrayList<Tuple2<String,String>> gd = getGroundTruth("twoWayGroundTruth0_1.txt");
        for(Tuple2<String,String> v : gd){
            boolean cont = results.contains(v);
            if(!cont) {
                System.out.format("(%s,%s): %b\n", v.f0, v.f1, cont);
            }
        }
        assertTrue(results.containsAll(getGroundTruth("twoWayGroundTruth0_1.txt")));
        assertTrue(getGroundTruth("twoWayGroundTruth0_1.txt").containsAll(results));

    }

    private static ArrayList<Tuple2<String,String>> getGroundTruth(String filename) throws Exception{
        ArrayList<Tuple2<String,String>> groudTruth = new ArrayList<>();

        try (Stream<String> lines = Files.lines(Paths.get(pwd + "/src/test/resources/"+ filename), Charset.defaultCharset())) {
            lines.map(l -> l.split(","))
                    .forEach(l -> groudTruth.add(new Tuple2<String,String>(l[0],l[1])));
        }

        return groudTruth;
    }

}
