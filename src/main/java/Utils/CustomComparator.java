package Utils;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Comparator;

public class CustomComparator implements Comparator<Tuple2<Integer,Double>> {

    @Override
    public int compare(Tuple2<Integer,Double> t1, Tuple2<Integer,Double> t2) {
        return Double.compare(t1.f1,t2.f1);
    }
}
