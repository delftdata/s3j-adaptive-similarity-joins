package CustomDataTypes;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;

import java.util.HashMap;
import java.util.List;

public class GroupFormulationState {

    public int key;

    public HashMap<Integer, Tuple2<Tuple3<Long, Integer, Double[]>, Integer>> mapping;

    public List<Tuple8<Integer,String,Integer,Long, Long,Integer,Double[],String>> physicalOuters;

    public String toString(){
        String s = "";
        s += "key: " + key + "\n";
        s += "physical outers: " + physicalOuters.toString() + "\n";
        s += "mapping: " + mapping.toString() + "\n";
        return s;
    }
}
