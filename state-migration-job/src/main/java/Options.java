import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import java.util.ArrayList;
import java.util.List;

public class Options {
    @Option(name="-minio", usage="Minio namenode: ip/hostname:port, e.g. localhost:9000.")
    private String minioLocation = "localhost:9000";

    @Option(name="-access", usage="Minio's access key.")
    private String minioAccessKey = "minio";

    @Option(name="-secret", usage="Minio's secret key.")
    private String minioSecretKey = "minio123";

    // All option-less arguments
    private String groupAllocFilename;
    private String groupAllocBucket;
    private String savepointName;
    private String stateBackendPath;

    @Argument
    private List<String> args = new ArrayList<String>();

    // Getters, setters, etc

    public String getMinioLocation() {
        return "http://" + minioLocation;
    }

    public void setArgs(){
        if(args.size() != 4){
            System.out.println("Invalid number of arguments.\n" +
                    "Three arguments with the following order are required:\n" +
                    "   1. The group allocation bucket\n" +
                    "   2. The group allocation object name\n" +
                    "   3. The savepoint name\n" +
                    "   4. The state backend path\n");
            System.exit(-1);
        }
        else{
            this.groupAllocBucket = args.get(0);
            this.groupAllocFilename = args.get(1);
            this.savepointName = args.get(2);
            this.stateBackendPath = args.get(3);
        }
    }

    public String getGroupAllocFilename(){return this.groupAllocFilename;}

    public String getGroupAllocBucket(){return this.groupAllocBucket;}

    public String getSavepointName(){return this.savepointName;}

    public String getStateBackendPath(){return this.stateBackendPath;}

    public String getMinioAccessKey() {
        return minioAccessKey;
    }

    public String getMinioSecretKey() {
        return minioSecretKey;
    }
}
