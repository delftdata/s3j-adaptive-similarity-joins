import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class StreamFactory {

    protected StreamExecutionEnvironment env;

    public StreamFactory(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public DataStream<Tuple3<Long, Integer, String>> createSimpleWordsStream() {
        String path = Resource.getPath("dummyStream.txt");
        DataStream<Tuple3<Long, Integer, String>> simpleWords = env.readTextFile(path,"UTF-8").map(new Parser());
        simpleWords = simpleWords.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Integer, String>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<Long, Integer, String> t) {
                return t.f0;
            }
        });
        return simpleWords;
    }
}
