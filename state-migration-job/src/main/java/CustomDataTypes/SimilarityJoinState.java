package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.HashMap;
import java.util.List;

public class SimilarityJoinState {

    public Tuple3<Integer, Integer, Integer> key;

    public HashMap<String, HashMap<String, List<FinalTuple>>> joinState;

    public String toString(){
        String s = "";
        s += "key: " + key + "\n";
        s += "joinState: " + joinState.toString() + "\n";
        return s;
    }
}
