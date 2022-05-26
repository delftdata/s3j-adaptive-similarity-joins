package Utils;


import CustomDataTypes.MinioConfiguration;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;

import java.io.*;

public class DatasetReader {

    private InputStream datasetStream;
    private BufferedReader bufferedReader;

    public DatasetReader(){}

    public DatasetReader(String datasetFile, MinioConfiguration minio){
        MinioClient minioClient =
                MinioClient.builder()
                        .endpoint(minio.getEndpoint())
                        .credentials(minio.getAccessKey(), minio.getSecretKey())
                        .build();
        try {
            this.datasetStream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket("embeddings")
                            .object(datasetFile)
                            .build());
        }
        catch (Exception e){
           e.printStackTrace();
           System.exit(-1);
        }
        this.bufferedReader = new BufferedReader( new InputStreamReader( datasetStream ) );

    }


    public BufferedReader getBufferedReader() {
        return bufferedReader;
    }

    public void closeConnection() throws Exception{
        this.datasetStream.close();
    }
}
