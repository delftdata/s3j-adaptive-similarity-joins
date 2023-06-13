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

    @Option(name="-threshold", usage="Similarity threshold to use.")
    private double threshold = 0.9;

    @Option(name="-monitoringWindow", usage="The length of the time window used for monitoring the pipeline.(In seconds)",
            required = true)
    private int windowLength;

    @Option(name="-processingWindow", usage = "Define the length of the processing window in seconds.")
    private int processingWindow = 60;

    @Option(name="-parallelism", usage="Define the desired level of parallelism. DEFAULT: 10")
    private int parallelism = 10;

    @Option(name="-virtualPartitions", usage="Define the number of virtual partitions. DEFAULT: 10")
    private int virtualPartitions = 10;

    // Getters, setters, etc
    public boolean hasSecondStream() { return twoStreams; }

    public int getCentroidsDim() {
        return centroidsDim;
    }

    public int getCentroidsNum() { return centroidsNum; }

    public double getThreshold() { return threshold; }

    public String getKafkaURL() { return kafkaURL; }

    public int getWindowLength() { return windowLength; }

    public int getParallelism() {
        return parallelism;
    }

    public int getProcessingWindow() {
        return processingWindow;
    }

    public int getVirtualPartitions() {
        return virtualPartitions;
    }
}
