import org.kohsuke.args4j.Option;

public class Options {

    @Option(name="-kafkaURL", usage="URL to kafka", required=true)
    private String kafkaURL = "";

    @Option(name="-windowLength", usage="Provide the length of the monitoring window.")
    private int windowLength = 120;

    @Option(name="-parallelism", usage="Define the desired level of parallelism. DEFAULT: 10")
    private int parallelism = 10;

    @Option(name="-jobID", usage="Provide the jobID of the monitored job.", required = true)
    private String jobID;

    public String getJobID() {
        return jobID;
    }

    public String getKafkaURL() {
        return kafkaURL;
    }

    public int getWindowLength() {
        return windowLength;
    }

    public int getParallelism() {
        return parallelism;
    }
}
