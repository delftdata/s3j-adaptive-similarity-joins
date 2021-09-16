package Parsers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;


public class Parser implements MapFunction<String, Tuple3<Long, Integer, String>> {


    @Override
    public Tuple3<Long, Integer, String> map(String s) throws Exception {
        String[] args = s.split(", ");
        return new Tuple3<Long, Integer, String>(Long.parseLong(args[0]), Integer.parseInt(args[1]), args[2]);
    }
}
