package Statistics;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

public class KeySelectors {

    public static class StatsKeySelector implements KeySelector<Tuple4<Integer, Integer, Boolean, Long>, Tuple3<Integer, Integer, Boolean>> {

        @Override
        public Tuple3<Integer, Integer, Boolean> getKey(Tuple4<Integer, Integer, Boolean, Long> t) throws Exception {
            return new Tuple3<Integer, Integer, Boolean>(t.f0, t.f1, t.f2);
        }
    }

    public static class PhyStatsKeySelector implements KeySelector<Tuple3<Integer, Boolean, Long>, Tuple2<Integer, Boolean>> {

        @Override
        public Tuple2<Integer, Boolean> getKey(Tuple3<Integer, Boolean, Long> t) throws Exception {
            return new Tuple2<Integer, Boolean>(t.f0, t.f1);
        }
    }

    public static class PhyStatsRtypeKeySelector implements KeySelector<Tuple3<Integer, String, Long>, Tuple2<Integer, String>>{

        @Override
        public Tuple2<Integer, String> getKey(Tuple3<Integer, String, Long> t) throws Exception {
            return new Tuple2<>(t.f0,t.f1);
        }
    }


    public static class BetweenPhyPartKeySelector implements KeySelector<Tuple4<Integer,Integer,Boolean,Long>,
            Tuple3<Integer, Integer, Boolean>>{


        @Override
        public Tuple3<Integer, Integer, Boolean> getKey(Tuple4<Integer, Integer, Boolean, Long> t) throws Exception {
            return new Tuple3<>(t.f0, t.f1, t.f2);
        }
    }

    public static class BetweenLogicalPartKeySelector implements KeySelector<Tuple5<Integer,Integer,Integer,Boolean,Long>,
            Tuple4<Integer, Integer, Integer, Boolean>>{


        @Override
        public Tuple4<Integer, Integer, Integer, Boolean> getKey(Tuple5<Integer, Integer, Integer, Boolean, Long> t) throws Exception {
            return new Tuple4<>(t.f0, t.f1, t.f2, t.f3);
        }
    }

    public static class RecordTypeLogicalPartitionSelector implements KeySelector<Tuple5<Long, Integer, Integer, String, Long>,
            Tuple3<Integer, Integer, String>>{


        @Override
        public Tuple3<Integer, Integer, String> getKey(Tuple5<Long, Integer, Integer, String, Long> t) throws Exception {
            return new Tuple3<>(t.f1, t.f2, t.f3);
        }
    }

}
