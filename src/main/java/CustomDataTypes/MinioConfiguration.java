package CustomDataTypes;

import java.io.Serializable;

public class MinioConfiguration implements Serializable {

    private String endpoint;
    private String accessKey;
    private String secretKey;

    public MinioConfiguration(){}

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
