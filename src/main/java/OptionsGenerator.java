import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import java.util.ArrayList;
import java.util.List;

public class OptionsGenerator {
    @Option(name="-kafkaURL", usage="URL to kafka")
    private String kafkaURL = "localhost:9002";

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

    public String getKafkaURL() { return kafkaURL; }
}
