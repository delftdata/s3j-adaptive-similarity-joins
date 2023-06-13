package Utils;

import CustomDataTypes.InputTuple;
import CustomDataTypes.MinioConfiguration;
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
import org.slf4j.Logger;

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

    public DataStream<InputTuple> create2DArrayStream(String pathToFile){
//        String path = Utils.Resource.getPath(inputFileName);
        DataStream<Tuple3<Long, Integer, Double[]>> initial = env.readTextFile(pathToFile,"UTF-8").map(new ArrayStreamParser());
        DataStream<InputTuple> arrays2D = initial.map(x -> new InputTuple(x.f0, System.currentTimeMillis(), x.f1, x.f2))
                .returns(TypeInformation.of(new TypeHint<InputTuple>() {}));
        arrays2D = arrays2D.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputTuple>() {
            @Override
            public long extractAscendingTimestamp(InputTuple t) {
                return t.f0;
            }
        });
        return arrays2D;
    }

    public DataStream<InputTuple> createGaussianMDStream(int seed, int rate, Long tmsp, int delay, int dimensions){
        DataStream<Tuple3<Long, Integer, Double[]>> initial = env.addSource(new GaussianMultiDimStreamGenerator(seed, rate, tmsp, delay, dimensions));
        DataStream<InputTuple> gaussianMD = initial.map(x -> new InputTuple(x.f0, System.currentTimeMillis(), x.f1, x.f2))
                .returns(TypeInformation.of(new TypeHint<InputTuple>() {}));
        gaussianMD = gaussianMD.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputTuple>() {
            @Override
            public long extractAscendingTimestamp(InputTuple t) {
                return t.f0;
            }
        });
        return gaussianMD;
    }

    public DataStream<InputTuple> createSkewedGaussian2DStream(int seed, int rate, Long tmsp, int delay){
        DataStream<Tuple3<Long, Integer, Double[]>> initial = env.addSource(new SkewedGaussian2DStreamGenerator(seed, rate, tmsp, delay));
        DataStream<InputTuple> skewed_gaussian2D = initial.map(x -> new InputTuple(x.f0, System.currentTimeMillis(), x.f1, x.f2))
                .returns(TypeInformation.of(new TypeHint<InputTuple>() {}));
        skewed_gaussian2D = skewed_gaussian2D.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputTuple>() {
            @Override
            public long extractAscendingTimestamp(InputTuple t) {
                return t.f0;
            }
        });
        return skewed_gaussian2D;
    }

    public DataStream<InputTuple> createUniformMDStream(int seed, int rate, Long tmsp, int delay, int dimensions){
        DataStream<Tuple3<Long, Integer, Double[]>> initial = env.addSource(new UniformMultiDimStreamGenerator(seed, rate, tmsp, delay, dimensions));
        DataStream<InputTuple> uniform = initial.map(x -> new InputTuple(x.f0, System.currentTimeMillis(), x.f1, x.f2))
                .returns(TypeInformation.of(new TypeHint<InputTuple>() {}));
        uniform = uniform.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputTuple>() {
            @Override
            public long extractAscendingTimestamp(InputTuple t) {
                return t.f0;
            }
        });
        return uniform;
    }

    public DataStream<InputTuple> createPareto2DStream(double scale, double shape, int rate, Long tmsp, int delay){
        DataStream<Tuple3<Long, Integer, Double[]>> initial = env.addSource(new Pareto2DStreamGenerator(scale, shape, rate, tmsp, delay));
        DataStream<InputTuple> pareto = initial.map(x -> new InputTuple(x.f0, System.currentTimeMillis(), x.f1, x.f2))
                .returns(TypeInformation.of(new TypeHint<InputTuple>() {}));
        pareto = pareto.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputTuple>() {
            @Override
            public long extractAscendingTimestamp(InputTuple t) {
                return t.f0;
            }
        });
        return pareto;
    }

    public DataStream<Tuple4<Long, Long, Integer, String>> createZipfianWordStream(String embeddingsFile, double zipfExp, int rate, Long tmsp, int delay, MinioConfiguration minio, Logger LOG){
        try {
            Set<String> wordSet = SimilarityJoinsUtil.readEmbeddings(embeddingsFile, minio, LOG).keySet();
            String[] words = wordSet.toArray(new String[0]);
            DataStream<Tuple3<Long, Integer, String>> initial = env.addSource(new ZipfianWordStreamGenerator(words, zipfExp, rate, tmsp, delay));
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

    public DataStream<Tuple4<Long, Long, Integer, String>> createUniformWordStream(String embeddingsFile, int seed, int rate, Long tmsp, int delay, MinioConfiguration minio, Logger LOG){
        try {
            Set<String> wordSet = SimilarityJoinsUtil.readEmbeddings(embeddingsFile, minio, LOG).keySet();
            String[] words = wordSet.toArray(new String[0]);
            DataStream<Tuple3<Long, Integer, String>> initial = env.addSource(new UniformWordStreamGenerator(words, seed, rate, tmsp, delay));
            DataStream<Tuple4<Long, Long, Integer,String>> uniformWords = initial.map(x -> new Tuple4<>(x.f0, System.currentTimeMillis(), x.f1, x.f2))
                    .returns(TypeInformation.of(new TypeHint<Tuple4<Long, Long, Integer, String>>() {}));
            uniformWords = uniformWords.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, Long, Integer, String>>() {
                @Override
                public long extractAscendingTimestamp(Tuple4<Long, Long, Integer, String> t) {
                    return t.f0;
                }
            });
            return uniformWords;
        }
        catch(Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    public DataStream<InputTuple> createIMDBStream(String imdbFile, int rate, Long tmsp, int delay, MinioConfiguration minio, Logger LOG){
        DataStream<Tuple3<Long, Integer, Double[]>> initial = env.addSource(new IMDBStreamGenerator(imdbFile, rate, tmsp, delay, minio));
        DataStream<InputTuple> imdbTitles = initial.map(x -> new InputTuple(x.f0, System.currentTimeMillis(), x.f1, x.f2))
                .returns(TypeInformation.of(new TypeHint<InputTuple>() {}));
        imdbTitles = imdbTitles.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputTuple>() {
            @Override
            public long extractAscendingTimestamp(InputTuple t) {
                return t.f0;
            }
        });
        return imdbTitles;
    }


    public DataStream<InputTuple> createDataStream(
            String source,
            int delay,
            int duration,
            int rate,
            MinioConfiguration minio,
            Logger LOG,
            int dimensions,
            String embeddingsFile,
            String imdbFile,
            int seed) throws Exception {

        DataStream<InputTuple> dataStream;
        int parallelismBefore = env.getParallelism();
        env.setParallelism(1);

        switch(source) {
            case "gaussian_2D_generator":
                dataStream = createGaussianMDStream(seed, rate, (long) duration, delay, 2);
                break;
            case "skewed_gaussian_2D_generator":
                dataStream = createSkewedGaussian2DStream(seed, rate, (long) duration, delay);
                break;
            case "uniform_2D_generator":
                dataStream = createUniformMDStream(seed, rate, (long) duration, delay, 2);
                break;
            case "pareto_2D_generator":
                dataStream = createPareto2DStream(1.0, 10.0 , rate, (long) duration, delay);
                break;
            case "zipfian_word_generator":
                dataStream =
                        createZipfianWordStream("1K_embeddings", 2.0, rate, (long) duration, delay, minio, LOG)
                                .map(new WordsToEmbeddingMapper("1K_embeddings", minio, LOG));
                break;
            case "uniform_word_generator":
                dataStream =
                        createUniformWordStream("1K_embeddings", 42, rate, (long) duration, delay, minio, LOG)
                                .map(new WordsToEmbeddingMapper("1K_embeddings", minio, LOG));
                break;
            case "uniform_MD_generator":
                dataStream = createUniformMDStream(seed, rate, (long) duration, delay, dimensions);
                break;
            case "gaussian_MD_generator":
                dataStream = createGaussianMDStream(seed, rate, (long) duration, delay, dimensions);
                break;
            case "imdb_stream_generator":
                dataStream = createIMDBStream(imdbFile, rate, (long) duration, delay, minio, LOG);
                break;
            default:
                dataStream = create2DArrayStream(source);

        }

        env.setParallelism(parallelismBefore);
        return dataStream;
    }

}
