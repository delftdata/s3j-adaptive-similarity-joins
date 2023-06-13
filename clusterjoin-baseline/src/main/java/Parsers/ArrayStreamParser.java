package Parsers;

import Utils.SimilarityJoinsUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class ArrayStreamParser implements MapFunction<String, Tuple3<Long, Integer, Double[]>> {

    @Override
    public Tuple3<Long, Integer, Double[]> map(String s) throws Exception {
        String[] args = s.split(", ", 3);
        return new Tuple3<Long, Integer, Double[]>(Long.parseLong(args[0]), Integer.parseInt(args[1]), SimilarityJoinsUtil.arrayStringToDouble(args[2].replaceAll("[\\]\\[]","").split(", ", 2),2));
    }
}
