package Utils;

import CustomDataTypes.InputTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

public class WordsToEmbeddingMapper implements MapFunction<Tuple4<Long, Long, Integer, String>, InputTuple> {

    HashMap<String, Double[]> wordEmbeddings;

    public WordsToEmbeddingMapper(String filename) throws Exception{
        this.wordEmbeddings = SimilarityJoinsUtil.readEmbeddings(filename);
    }


    @Override
    public InputTuple map(Tuple4<Long, Long, Integer, String> t) throws Exception {
        Double[] embedding = new Double[300];
        Arrays.fill(embedding, 0.0);
        String[] sentence = t.f3.split(" ");
        Set<String> keys = wordEmbeddings.keySet();
        int sum = 0;
        for(String word : sentence){
            if(keys.contains(word)){
                sum += 1;
                Double[] tmp = wordEmbeddings.get(word);
                for(int i=0; i < 300; i++){
                    embedding[i] += tmp[i];
                }
            }
        }
        if(sum != 0) {
            for (int i = 0; i < 300; i++) {
                embedding[i] = embedding[i] / sum;
            }
        }
        return new InputTuple(t.f0, t.f1, t.f2, embedding);
    }
}
