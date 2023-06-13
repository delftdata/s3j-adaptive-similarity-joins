package Operators;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;

public class SimilarityJoinSelf extends SimilarityJoin {
    public SimilarityJoinSelf(Double dist_thresh) throws Exception {
        super(dist_thresh);
    }

    @Override
    public boolean isSelfJoin() {
        return true;
    }
}
