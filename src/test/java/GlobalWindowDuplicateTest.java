import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class GlobalWindowDuplicateTest {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalWindowCorrectnessTest.class);

    static String pwd = Paths.get("").toAbsolutePath().toString();

    @Test
    public void testDuplicateResults() throws Exception{

        PipelineToTest pipeline = new PipelineToTest();
        List<Tuple2<Integer,Integer>> results = pipeline.run(20, "wordStream.txt");

        assertFalse(hasDuplicate(results));
    }

    // Code adapted from https://stackoverflow.com/a/600319
    public static boolean hasDuplicate(List<Tuple2<Integer,Integer>> all) {
        boolean result = false;
        Set<Tuple2<Integer,Integer>> set = new HashSet<Tuple2<Integer,Integer>>();
        // Set#add returns false if the set does not change, which
        // indicates that a duplicate element has been added.
        for (Tuple2<Integer,Integer> each: all) if (!set.add(each)){
            System.out.println(each.toString());
            result = true;
        }
        return result;
    }

}