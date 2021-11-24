import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import java.util.ArrayList;
import java.util.List;

public class Options {
    @Option(name="-kafkaURL", usage="URL to kafka", required=true)
    private String kafkaURL = "";

    @Option(name="-twoStreams", usage="Whether to expect two streams")
    private boolean twoStreams = false;

    @Option(name="-centroidsDim", usage="Dimension of the centroids, use 300 for zipfian word stream.")
    private int centroidsDim = 2;

    @Option(name="-centroidsNum", usage="Number of centroids to use.")
    private int centroidsNum = 10;


    // Getters, setters, etc
    public boolean hasSecondStream() { return twoStreams; }

    public int getCentroidsDim() {
        return centroidsDim;
    }

    public int getCentroidsNum() {
        return centroidsNum;
    }

    public String getKafkaURL() { return kafkaURL; }
}
