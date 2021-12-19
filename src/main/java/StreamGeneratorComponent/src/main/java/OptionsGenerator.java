import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import java.util.ArrayList;
import java.util.List;

public class OptionsGenerator {

    @Option(name="-kafkaURL", usage="URL to Kafka")
    private String kafkaURL = "localhost:9092";

    @Option(name="-delay", usage="Specify the delay between generated data.")
    private int delay = 1;

    @Option(name="-rate", usage="The rate of datapoints per time unit.")
    private int rate = 1000;

    @Option(name="-duration", usage="The duration of the stream")
    private int duration = 10;

    // All option-less arguments
    @Argument
    private List<String> streams = new ArrayList<>();

    // Getters, setters, etc
    public boolean hasSecondStream() { return streams.size() > 1; }

    public String getFirstStream() {
        return streams.get(0);
    }

    public String getSecondStream() {
        return streams.get(1);
    }

    public String getKafkaURL() {
        return kafkaURL;
    }

    public int getDelay() {
        return delay;
    }

    public int getDuration() {
        return duration;
    }

    public int getRate() {
        return rate;
    }
}
