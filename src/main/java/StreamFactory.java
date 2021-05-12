import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

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

    public DataStream<Tuple3<Long, Integer, Double[]>> create2DArrayStream(String inputFileName){
        String path = Resource.getPath(inputFileName);
        DataStream<Tuple3<Long, Integer, Double[]>> arrays2D = env.readTextFile(path,"UTF-8").map(new ArrayStreamParser());
        arrays2D = arrays2D.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Integer, Double[]>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<Long, Integer, Double[]> t) {
                return t.f0;
            }
        });
        return arrays2D;
    }
}
