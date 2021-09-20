import Generators.*;
import Parsers.ArrayStreamParser;
import Parsers.Parser;
import Utils.SimilarityJoinsUtil;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.util.Set;

public class StreamFactory {

    protected StreamExecutionEnvironment env;

    public StreamFactory(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public DataStream<Tuple3<Long, Integer, String>> createSimpleWordsStream(String inputFileName) {
        String path = Resource.getPath(inputFileName);
        DataStream<Tuple3<Long, Integer, String>> simpleWords = env.readTextFile(path,"UTF-8").map(new Parser());
        simpleWords = simpleWords.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Integer, String>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<Long, Integer, String> t) {
                return t.f0;
            }
        });
        return simpleWords;
    }

    public DataStream<Tuple3<Long, Integer, Double[]>> create2DArrayStream(String pathToFile){
//        String path = Resource.getPath(inputFileName);
        DataStream<Tuple3<Long, Integer, Double[]>> arrays2D = env.readTextFile(pathToFile,"UTF-8").map(new ArrayStreamParser());
        arrays2D = arrays2D.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Integer, Double[]>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<Long, Integer, Double[]> t) {
                return t.f0;
            }
        });
        return arrays2D;
    }

    public DataStream<Tuple3<Long, Integer, Double[]>> createGaussian2DStream(int seed, int rate, Long tmsp){
        DataStream<Tuple3<Long, Integer, Double[]>> gaussian2D = env.addSource(new Gaussian2DStreamGenerator(seed, rate, tmsp));
        gaussian2D = gaussian2D.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Integer, Double[]>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<Long, Integer, Double[]> t) {
                return t.f0;
            }
        });
        return gaussian2D;
    }

    public DataStream<Tuple3<Long, Integer, Double[]>> createSkewedGaussian2DStream(int seed, int rate, Long tmsp){
        DataStream<Tuple3<Long, Integer, Double[]>> skewed_gaussian2D = env.addSource(new SkewedGaussian2DStreamGenerator(seed, rate, tmsp));
        skewed_gaussian2D = skewed_gaussian2D.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Integer, Double[]>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<Long, Integer, Double[]> t) {
                return t.f0;
            }
        });
        return skewed_gaussian2D;
    }

    public DataStream<Tuple3<Long, Integer, Double[]>> createUniform2DStream(int seed, int rate, Long tmsp){
        DataStream<Tuple3<Long, Integer, Double[]>> uniform = env.addSource(new Uniform2DStreamGenerator(seed, rate, tmsp));
        uniform = uniform.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Integer, Double[]>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<Long, Integer, Double[]> t) {
                return t.f0;
            }
        });
        return uniform;
    }

    public DataStream<Tuple3<Long, Integer, Double[]>> createPareto2DStream(double scale, double shape, int rate, Long tmsp){
        DataStream<Tuple3<Long, Integer, Double[]>> pareto = env.addSource(new Pareto2DStreamGenerator(scale, shape, rate, tmsp));
        pareto = pareto.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Integer, Double[]>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<Long, Integer, Double[]> t) {
                return t.f0;
            }
        });
        return pareto;
    }

    public DataStream<Tuple3<Long, Integer, String>> createZipfianWordStream(String embeddingsFile, double zipfExp, int rate, Long tmsp){
        try {
            Set<String> wordSet = SimilarityJoinsUtil.readEmbeddings(embeddingsFile).keySet();
            String[] words = wordSet.toArray(new String[0]);
            DataStream<Tuple3<Long, Integer, String>> zipfian = env.addSource(new ZipfianWordStreamGenerator(words, zipfExp, rate, tmsp));
            zipfian = zipfian.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Integer, String>>() {
                @Override
                public long extractAscendingTimestamp(Tuple3<Long, Integer, String> t) {
                    return t.f0;
                }
            });
            return zipfian;
        }
        catch(Exception e){
            System.out.println(e.getMessage());
            System.exit(-1);
        }
        return null;
    }

}
