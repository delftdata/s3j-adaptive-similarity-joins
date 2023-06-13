package Statistics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;
import java.util.List;

public class FindCandidateNodes implements MapFunction<List<Tuple3<Long, Integer, Long>>, Tuple4<Long, Long, List<Integer>, List<Integer>>> {

    @Override
    public Tuple4<Long, Long, List<Integer>, List<Integer>> map(List<Tuple3<Long, Integer, Long>> tuples) throws Exception {
        int sz = tuples.size();
        Long sum = 0L;
        Long tmsp = tuples.get(0).f0;

        for(Tuple3<Long, Integer, Long> t : tuples){
            sum += t.f2;
        }

        Long average = sum/sz;

        List<Integer> over = new ArrayList<>();
        List<Integer> under = new ArrayList<>();

        for(Tuple3<Long, Integer, Long> t : tuples){
            if(t.f2 < 0.8 * average){
                under.add(t.f1);
            }
            else if (t.f2 > 1.2 * average){
                over.add(t.f1);
            }
        }

        return new Tuple4<>(tmsp,average,over,under);

    }
}
