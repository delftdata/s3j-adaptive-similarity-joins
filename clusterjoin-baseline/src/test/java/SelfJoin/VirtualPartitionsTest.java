package SelfJoin;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class VirtualPartitionsTest {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalWindowCorrectnessTest.class);

    static String pwd = Paths.get("").toAbsolutePath().toString();

    @Test
    public void testVirtualPartitions() throws Exception {

        VirtualPartitionsPipeline pipeline = new VirtualPartitionsPipeline();

        List<Tuple2<Integer, Integer>> results =
                pipeline.run(
                        10,
                        10,
                        "src/test/resources/1K_2D_Array_Stream_v2.txt",
                        LOG
                );

        ArrayList<Tuple2<Integer, Integer>> gd = getGroundTruth("1K_2D_Array_Stream_v2GroundTruth0_05.txt");
        for (Tuple2<Integer, Integer> v : gd) {
            boolean cont = results.contains(v);
            if (!cont) {
                System.out.format("(%d,%d): %b\n", v.f0, v.f1, cont);
            }
        }
        assertTrue(results.containsAll(getGroundTruth("1K_2D_Array_Stream_v2GroundTruth0_05.txt")));
        assertTrue(getGroundTruth("1K_2D_Array_Stream_v2GroundTruth0_05.txt").containsAll(results));
    }

        private static ArrayList<Tuple2<Integer,Integer>> getGroundTruth(String filename) throws Exception{
            ArrayList<Tuple2<Integer,Integer>> groudTruth = new ArrayList<>();

            try (Stream<String> lines = Files.lines(Paths.get(pwd + "/src/test/resources/"+ filename), Charset.defaultCharset())) {
                lines.map(l -> l.split(","))
                        .forEach(l -> groudTruth.add(new Tuple2<Integer,Integer>(Integer.parseInt(l[0]),Integer.parseInt(l[1]))));
            }

            return groudTruth;
        }

}
