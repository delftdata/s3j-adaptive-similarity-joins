package Operators;

public class SimilarityJoinSelf extends SimilarityJoin {
    public SimilarityJoinSelf(Double dist_thresh, boolean blas) throws Exception {
        super(dist_thresh, blas);
    }

    @Override
    public boolean isSelfJoin() {
        return true;
    }
}
