import CustomDataTypes.MinioConfiguration;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import java.util.ArrayList;
import java.util.List;

public class OptionsGenerator {

    @Option(name="-kafkaURL", usage="URL to Kafka")
    private String kafkaURL = "localhost:9092";

    @Option(name="-delay", usage="Delay between generated data.")
    private int delay = 1;

    @Option(name="-rate", usage="Rate of datapoints per time unit.")
    private int rate = 1000;

    @Option(name="-dimensions", usage="Dimensions of the the records' values.")
    private int dimensions = 2;

    @Option(name="-duration", usage="Duration of the stream")
    private int duration = 10;

    @Option(name="-minioEndpoint", usage="Endpoint to connect to MinIO.")
    private String minioEndpoint = "localhost:9000";

    @Option(name="-minioAccessKey", usage="Access key for MinIO.")
    private String minioAccessKey = "minio";

    @Option(name="-minioSecretKey", usage="Secret key for MinIO")
    private String minioSecretKey = "minio123";

    @Option(name="-embeddings", usage="Embeddings to be used.")
    private String embeddingsFile= "1K_embeddings";

    @Option(name="-seed", usage="Seed for the random generator.")
    private int seed = 42;

    @Option(name="-dataset", usage="The dataset to be streamed.")
    private String dataset;

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

    public MinioConfiguration getMinio(){
        return new MinioConfiguration(minioEndpoint, minioAccessKey, minioSecretKey);
    }

    public int getDimensions() {
        return dimensions;
    }

    public String getEmbeddingsFile() {
        return embeddingsFile;
    }

    public int getSeed() {
        return seed;
    }

    public String getDataset() {
        return dataset;
    }

}
