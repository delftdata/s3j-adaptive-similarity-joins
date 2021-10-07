import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import java.util.ArrayList;
import java.util.List;

public class Options {
    @Option(name="-centroidsDim", usage="Dimension of the centroids, use 300 for zipfian word stream.")
    private final int centroidsDim = 2;

    @Option(name="-centroidsNum", usage="Number of centroids to use.")
    private final int centroidsNum = 10;

    // All option-less arguments
    @Argument
    private final List<String> streams = new ArrayList<String>();

    // Getters, setters, etc
    public String getFirstStream() {
        return streams.get(0);
    }

    public boolean hasSecondStream() {
        return streams.size() > 1;
    }

    public String getSecondStream() {
        return streams.get(1);
    }

    public int getCentroidsDim() {
        return centroidsDim;
    }

    public int getCentroidsNum() {
        return centroidsNum;
    }
}
