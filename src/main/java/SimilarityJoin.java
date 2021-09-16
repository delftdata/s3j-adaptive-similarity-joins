import Utils.SimilarityJoinsUtil;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.LinkedList;

import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;


public class SimilarityJoin extends ProcessWindowFunction<Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>,
        Tuple3<Boolean, Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>,Tuple10<Integer,String,Integer,String,Integer,Integer,Long,Integer,Double[],Integer>>,
        Tuple3<Integer,Integer,Integer>,
        GlobalWindow> {

    Double dist_thresh;
    private Logger LOG;
    OutputTag<Tuple3<Long, Integer, Integer>> sideJoins;

    public SimilarityJoin(Double dist_thresh, Logger LOG, OutputTag<Tuple3<Long, Integer, Integer>> sideJoins)throws Exception{
        this.dist_thresh = dist_thresh;
        this.LOG = LOG;
        this.sideJoins = sideJoins;
    }


    @Override
    public void process(Tuple3<Integer,Integer,Integer> key,
                        Context ctx,
                        Iterable<Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[],Integer>> tuples,
                        Collector<Tuple3<Boolean,
                                Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer>,
                                Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer>>> collector)
            throws Exception {

        Iterator<Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer>> tuplesIterator = tuples.iterator();
        LinkedList<Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer>> tuplesList = new LinkedList<>();
        tuplesIterator.forEachRemaining(tuplesList::addFirst);

        Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer> newTuple = tuplesList.pollFirst();
        Double[] newTupleEmbed = newTuple.f8;

        for (Tuple10<Integer, String, Integer, String, Integer, Integer, Long, Integer, Double[], Integer> t : tuplesList ) {

//            LOG.info(newTuple.toString()+", "+t.toString());
            boolean exp = (
                    (newTuple.f1.equals("outer") && t.f1.equals("inner")) ||
                            (newTuple.f1.equals("outlier") && t.f1.equals("outlier")) ||
                            (newTuple.f1.equals("outlier") && t.f1.equals("inner")) ||
                            (newTuple.f1.equals("inner") && t.f1.equals("outlier")) ||
                            (newTuple.f1.equals("inner") && t.f1.equals("outer")) ||
                            (newTuple.f1.equals("outlier") && t.f1.equals("outer")) ||
                            (newTuple.f1.equals("outer") && t.f1.equals("outlier")) ||
//                            (newTuple.f1.equals("outlier") && t.f1.equals("outer") && !t.f7.equals(newTuple.f7)  && (newTuple.f0 < t.f5 || t.f3.equals("pOuter"))) ||
//                            (newTuple.f1.equals("outer") && t.f1.equals("outlier") && !t.f7.equals(newTuple.f7) && (newTuple.f5 > t.f0 || newTuple.f3.equals("pOuter"))) ||
                            (newTuple.f1.equals("ind_outer") && t.f1.equals("inner")) ||
                            (newTuple.f1.equals("inner") && t.f1.equals("ind_outer"))
            );

            if (exp){
                ctx.output(sideJoins, new Tuple3<>(newTuple.f6, newTuple.f2, newTuple.f0));
                Double[] tEmbed = t.f8;
                if(newTuple.f7 > t.f7) {
                    collector.collect(
                            new Tuple3<>(
                                    (SimilarityJoinsUtil.AngularDistance(newTupleEmbed, tEmbed) < dist_thresh),
                                    newTuple,
                                    t
                            )
                    );
                }
                else{
                    collector.collect(
                            new Tuple3<>(
                                    (SimilarityJoinsUtil.AngularDistance(newTupleEmbed, tEmbed) < dist_thresh),
                                    t,
                                    newTuple
                            )
                    );
                }
            }
            else if(newTuple.f1.equals("inner") && t.f1.equals("inner")){
                if(newTuple.f7 > t.f7) {
                    collector.collect(
                            new Tuple3<>(
                                    true,
                                    newTuple,
                                    t
                            )
                    );
                }
                else{
                    collector.collect(
                            new Tuple3<>(
                                    true,
                                    t,
                                    newTuple
                            )
                    );
                }
            }
        }
    }
}