package Operators;

public class SimilarityJoinSelfCJ extends SimilarityJoinCJ {

    public SimilarityJoinSelfCJ(Double dist_threshold) throws Exception {
        super(dist_threshold);
    }

    @Override
    public boolean isSelfJoin() {
        return true;
    }

}
