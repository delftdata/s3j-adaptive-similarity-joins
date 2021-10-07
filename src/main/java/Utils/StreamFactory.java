package Utils;

import Generators.*;
import Parsers.ArrayStreamParser;
import Parsers.Parser;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.util.Set;

public class StreamFactory {

    protected StreamExecutionEnvironment env;

    public StreamFactory(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public DataStream<Tuple4<Long, Long, Integer, String>> createSimpleWordsStream(String inputFileName) {
        String path = Resource.getPath(inputFileName);
        DataStream<Tuple3<Long, Integer, String>> initial = env.readTextFile(path,"UTF-8").map(new Parser());
        DataStream<Tuple4<Long, Long, Integer, String>> simpleWords = initial.map(x -> new Tuple4<>(x.f0, System.currentTimeMillis(), x.f1, x.f2))
                .returns(TypeInformation.of(new TypeHint<Tuple4<Long, Long, Integer, String>>() {}));
        simpleWords = simpleWords.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, Long, Integer, String>>() {
            @Override
            public long extractAscendingTimestamp(Tuple4<Long, Long, Integer, String> t) {
                return t.f0;
            }
        });
        return simpleWords;
    }

    public DataStream<Tuple4<Long, Long, Integer, Double[]>> create2DArrayStream(String pathToFile){
//        String path = Utils.Resource.getPath(inputFileName);
        DataStream<Tuple3<Long, Integer, Double[]>> initial = env.readTextFile(pathToFile,"UTF-8").map(new ArrayStreamParser());
        DataStream<Tuple4<Long, Long, Integer, Double[]>> arrays2D = initial.map(x -> new Tuple4<>(x.f0, System.currentTimeMillis(), x.f1, x.f2))
                .returns(TypeInformation.of(new TypeHint<Tuple4<Long, Long, Integer, Double[]>>() {}));
        arrays2D = arrays2D.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, Long, Integer, Double[]>>() {
            @Override
            public long extractAscendingTimestamp(Tuple4<Long, Long, Integer, Double[]> t) {
                return t.f0;
            }
        });
        return arrays2D;
    }

    public DataStream<Tuple4<Long, Long, Integer, Double[]>> createGaussian2DStream(int seed, int rate, Long tmsp){
        DataStream<Tuple3<Long, Integer, Double[]>> initial = env.addSource(new Gaussian2DStreamGenerator(seed, rate, tmsp));
        DataStream<Tuple4<Long, Long, Integer, Double[]>> gaussian2D = initial.map(x -> new Tuple4<>(x.f0, System.currentTimeMillis(), x.f1, x.f2))
                .returns(TypeInformation.of(new TypeHint<Tuple4<Long, Long, Integer, Double[]>>() {}));
        gaussian2D = gaussian2D.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, Long, Integer, Double[]>>() {
            @Override
            public long extractAscendingTimestamp(Tuple4<Long, Long, Integer, Double[]> t) {
                return t.f0;
            }
        });
        return gaussian2D;
    }

    public DataStream<Tuple4<Long, Long, Integer, Double[]>> createSkewedGaussian2DStream(int seed, int rate, Long tmsp){
        DataStream<Tuple3<Long, Integer, Double[]>> initial = env.addSource(new SkewedGaussian2DStreamGenerator(seed, rate, tmsp));
        DataStream<Tuple4<Long, Long, Integer, Double[]>> skewed_gaussian2D = initial.map(x -> new Tuple4<>(x.f0, System.currentTimeMillis(), x.f1, x.f2))
                .returns(TypeInformation.of(new TypeHint<Tuple4<Long, Long, Integer, Double[]>>() {}));
        skewed_gaussian2D = skewed_gaussian2D.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, Long, Integer, Double[]>>() {
            @Override
            public long extractAscendingTimestamp(Tuple4<Long, Long, Integer, Double[]> t) {
                return t.f0;
            }
        });
        return skewed_gaussian2D;
    }

    public DataStream<Tuple4<Long, Long, Integer, Double[]>> createUniform2DStream(int seed, int rate, Long tmsp){
        DataStream<Tuple3<Long, Integer, Double[]>> initial = env.addSource(new Uniform2DStreamGenerator(seed, rate, tmsp));
        DataStream<Tuple4<Long, Long, Integer, Double[]>> uniform = initial.map(x -> new Tuple4<>(x.f0, System.currentTimeMillis(), x.f1, x.f2))
                .returns(TypeInformation.of(new TypeHint<Tuple4<Long, Long, Integer, Double[]>>() {}));
        uniform = uniform.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, Long, Integer, Double[]>>() {
            @Override
            public long extractAscendingTimestamp(Tuple4<Long, Long, Integer, Double[]> t) {
                return t.f0;
            }
        });
        return uniform;
    }

    public DataStream<Tuple4<Long, Long, Integer, Double[]>> createPareto2DStream(double scale, double shape, int rate, Long tmsp){
        DataStream<Tuple3<Long, Integer, Double[]>> initial = env.addSource(new Pareto2DStreamGenerator(scale, shape, rate, tmsp));
        DataStream<Tuple4<Long, Long, Integer, Double[]>> pareto = initial.map(x -> new Tuple4<>(x.f0, System.currentTimeMillis(), x.f1, x.f2))
                .returns(TypeInformation.of(new TypeHint<Tuple4<Long, Long, Integer, Double[]>>() {}));
        pareto = pareto.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, Long, Integer, Double[]>>() {
            @Override
            public long extractAscendingTimestamp(Tuple4<Long, Long, Integer, Double[]> t) {
                return t.f0;
            }
        });
        return pareto;
    }

    public DataStream<Tuple4<Long, Long, Integer, String>> createZipfianWordStream(String embeddingsFile, double zipfExp, int rate, Long tmsp){
        try {
            Set<String> wordSet = SimilarityJoinsUtil.readEmbeddings(embeddingsFile).keySet();
            String[] words = wordSet.toArray(new String[0]);
            DataStream<Tuple3<Long, Integer, String>> initial = env.addSource(new ZipfianWordStreamGenerator(words, zipfExp, rate, tmsp));
            DataStream<Tuple4<Long, Long, Integer,String>> zipfian = initial.map(x -> new Tuple4<>(x.f0, System.currentTimeMillis(), x.f1, x.f2))
                    .returns(TypeInformation.of(new TypeHint<Tuple4<Long, Long, Integer, String>>() {}));
            zipfian = zipfian.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, Long, Integer, String>>() {
                @Override
                public long extractAscendingTimestamp(Tuple4<Long, Long, Integer, String> t) {
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


    public DataStream<Tuple4<Long,Long,Integer,Double[]>> createDataStream(String source) throws Exception {
        DataStream<Tuple4<Long, Long, Integer, Double[]>> dataStream;

        switch(source) {
            case "gaussian_2D_generator":           dataStream = createGaussian2DStream(42, 1000, 10L);
                break;
            case "skewed_gaussian_2D_generator":    dataStream = createSkewedGaussian2DStream(42, 1000, 10L);
                break;
            case "uniform_2D_generator":            dataStream = createUniform2DStream(42, 1000, 10L);
                break;
            case "pareto_2D_generator":             dataStream = createPareto2DStream(1.0, 10.0 , 1000, 10L);
                break;
            case "zipfian_word_generator":          dataStream = createZipfianWordStream("wiki-news-300d-1K.vec", 2.0, 1000, 10L)
                    .map(new WordsToEmbeddingMapper("wiki-news-300d-1K.vec"));
                break;
            default:                                dataStream = create2DArrayStream(source);

        }

        return dataStream;
    }

}
