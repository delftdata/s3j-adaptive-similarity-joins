package CustomDataTypes;

public class MinioConfiguration {

    private final String endpoint;
    private final String accessKey;
    private final String secretKey;

    public MinioConfiguration(String endpoint, String accessKey, String secretKey){
        this.endpoint = "http://"+endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }


    public String getEndpoint() {
        return endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }
}
