package Utils;

import CustomDataTypes.InputTuple;
import CustomDataTypes.MinioConfiguration;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.distribution.ParetoDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.random.Well19937c;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class SimilarityJoinsUtil {

    static String pwd = Paths.get("").toAbsolutePath().toString();

    public static Predicate<String> isContained(Set<String> embeddingsKeys){

        return p -> {
            String[] words = p.split("\\s+");
            for(String w : words){
                if(!w.equals("Episode")) {
                    if (embeddingsKeys.contains(w)) {
                        return true;
                    }
                }
            }
            return false;
        };
    }

//    public static Predicate<String> isField(Set<String> embeddingsKeys, String field){
//
//        return p -> {
//            if p
//        };
//    }

    private static final Logger LOG = LoggerFactory.getLogger(SimilarityJoinsUtil.class);

    public static HashMap<Integer, Double[]> RandomCentroids(int numCentroids, int dimension){

        Random rand = new Random(42);
        HashMap<Integer, Double[]> centroids = new HashMap<Integer, Double[]>();

        for(int i=0; i<numCentroids; i++){
            Double[] cent = new Double[dimension];
            for(int j=0; j<dimension; j++){
                cent[j] = rand.nextDouble()*2 - 1;
            }
            centroids.put(i, cent);
        }

        return centroids;
    }

    public static Double AngularDistance(Double[] vectorA, Double[] vectorB){
        double p = Math.PI;
        Double cos_sim = CosineSimilarity(vectorA, vectorB);
        return Math.acos(cos_sim)/p;
    }

    public static Double CosineSimilarity(Double[] vectorA, Double[] vectorB){
        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        try {
            for (int i = 0; i < vectorA.length; i++) {
                dotProduct += vectorA[i] * vectorB[i];
                normA += Math.pow(vectorA[i], 2);
                normB += Math.pow(vectorB[i], 2);
            }
        }
        catch(Exception e){
            System.out.println(Arrays.toString(vectorA));
            System.out.println(Arrays.toString(vectorB));
            throw e;
        }
        double csim = dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
        return Math.min(csim, 1.0);
    }

    public static Double CosineDistance(Double[] vectorA, Double[] vectorB){
        return 1 - CosineSimilarity(vectorA, vectorB);
    }

    public static Double EuclideanDistance(Double[] vectorA, Double[] vectorB){
        double[] vecA = ArrayUtils.toPrimitive(vectorA);
        double[] vecB = ArrayUtils.toPrimitive(vectorB);
        return new org.apache.commons.math3.ml.distance.EuclideanDistance().compute(vecA, vecB);
    }

    public static Double[] arrayStringToDouble(String[] stringArray, int dimension){
        Double[] doubleArray = new Double[dimension];
        int counter = 0;
        for(String s : stringArray) {
            doubleArray[counter] = Double.parseDouble(s);
            counter++;
        }
        return doubleArray;
    }

    public static double nextSkewedBoundedDouble(double min, double max, double skew, double bias, Random rand) {
        double range = max - min;
        double mid = min + range / 2.0;
        double unitGaussian = rand.nextGaussian();
        double biasFactor = Math.exp(bias);
        double retval = mid+(range*(biasFactor/(biasFactor+Math.exp(-unitGaussian/skew))-0.5));
        return retval;
    }

    public static void createZipfianWordStream(Set<String> wordSet, int rate, int tmsp) throws IOException {
        List<String> words = new ArrayList<String>(wordSet);
        Random rand = new Random(42);
        Collections.shuffle(words, rand);

        try{
            FileWriter myWriter = new FileWriter(pwd + "/src/main/resources/zipfianWordStream.txt");
            Random randRate = new Random(70);
            Well19937c randZipf = new Well19937c(3);
            ZipfDistribution zipf = new ZipfDistribution(randZipf, words.size(), 2.0);
            long id = 0;
            for(int t = 0; t < tmsp; t++){
                int tRate = (int) (randRate.nextInt((int)(0.2*rate)) + 0.95*rate);
                System.out.format("%d, %d\n", t, tRate);
                for(int i = 0; i < tRate; i++){
                    int wordIndex = zipf.sample()-1;
                    String nextStreamItem = words.get(wordIndex);
                    String toWrite = String.format("%d, %d, %s\n", t*1000, id, nextStreamItem);
                    myWriter.write(toWrite);
                    id++;
                }
            }
            myWriter.close();
        }
        catch(Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw e;
        }

    }

    public static void create2DParetoStream(int rate, int tmsp) throws IOException {
        try {
            FileWriter myWriter = new FileWriter(pwd + "/src/main/resources/2DParetoStream.txt");
            Random randRate = new Random(70);
            Well19937c randPareto = new Well19937c(42);
            ParetoDistribution pareto = new ParetoDistribution(randPareto, 5, 5);
            long id = 0;
            for(int t = 0; t < tmsp; t++){
                int tRate = (int) (randRate.nextInt((int)(0.2*rate)) + 0.95*rate);
                System.out.format("%d, %d\n", t, tRate);
                for(int i = 0; i < tRate; i++){
                    double x = pareto.sample();
                    double y = pareto.sample();
                    String nextStreamItem = new Tuple2<Double, Double>(x,y).toString();
                    String toWrite = String.format("%d, %d, %s\n", t*1000, id, nextStreamItem);
                    myWriter.write(toWrite);
                    id++;
                }
            }
            myWriter.close();
        }
        catch(Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    public static void createComplex2DStream(int rate, int tmsp) throws IOException {
        try {
            FileWriter myWriter = new FileWriter(pwd + "/src/main/resources/Complex2DStream.txt");
            Random randRate = new Random(70);
            Well19937c randZipf = new Well19937c(42);
            List<ParetoTopic2D> topics = new ArrayList<>();
            topics.add(new ParetoTopic2D(0.1, 0.5, 10, 10, 1, 1));
            topics.add(new ParetoTopic2D(0.5, 0.1, 10, 10, 1, 1));
            topics.add(new ParetoTopic2D(0.5, 0.5, 10, 10, -1, -1));
            topics.add(new ParetoTopic2D(0.5, 0.5, 10, 10, 1, 1));
            topics.add(new ParetoTopic2D(0.1, 0.5, 10, 10, -1, -1));
            topics.add(new ParetoTopic2D(0.5, 0.1, 10, 10, -1, -1));
            topics.add(new ParetoTopic2D(0.1, 0.5, 10, 10, 1, -1));
            topics.add(new ParetoTopic2D(0.5, 0.1, 10, 10, -1, 1));

            ZipfDistribution zipf = new ZipfDistribution(randZipf, topics.size(), 2.0);
            int id = 0;

            for(int t = 0; t < tmsp; t++){
                int tRate = (int) (randRate.nextInt((int)(0.2*rate)) + 0.95*rate);
                System.out.format("%d, %d\n", t, tRate);
                for(int i = 0; i < tRate; i++){
                    int zipfSample = zipf.sample() - 1;

                    String nextStreamItem = topics.get(zipfSample).getParetoTuple().toString();
                    String toWrite = String.format("%d, %d, %d, %s\n", t*1000, id, zipfSample, nextStreamItem);
                    myWriter.write(toWrite);
                    id++;
                }
            }
            myWriter.close();
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    public static void createStreamFile(HashMap<String, Double[]> embeddings, int N) throws Exception{

        Set<String> setKeys = embeddings.keySet();
        int arrSize = setKeys.size();
        String[] keys = setKeys.toArray(new String[arrSize]);

        try {
            FileWriter myWriter = new FileWriter(pwd + "/src/main/resources/1KwordStream_v2.txt");
            Random rand = new Random(1000);
            int timestamp = 0;
            int id = 0;
            int itemPerStamp = 20;
            for(int i = 0; i < N; i++){
                if(itemPerStamp == 0){
                    itemPerStamp = 20 + rand.nextInt(80);
                    timestamp++;
                }

                int k = 1 + rand.nextInt(999);
                String nextStreamItem = keys[k];
                String toWrite = String.format("%d, %d, %s\n", timestamp, id, nextStreamItem);
                myWriter.write(toWrite);

                id++;
                itemPerStamp--;
            }
            myWriter.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    public static void create2DArrayStream(int N) throws Exception{
        try {
            FileWriter myWriter = new FileWriter(pwd + "/src/main/resources/1K_2D_Array_Stream_v2.txt");
            Random rand = new Random(1000);
            int itemPerStamp = 20;
            int timestamp = 0;
            int id = 0;
            for(int i = 0; i < N; i++){
                if(itemPerStamp == 0){
                    itemPerStamp = 20 + rand.nextInt(80);
                    timestamp += 1000;
                }

                Double[] nextStreamItem = new Double[2];
//                nextStreamItem[0] = nextSkewedBoundedDouble(0, 10, 1, 0, rand) * 2 - 10;
//                nextStreamItem[1] = nextSkewedBoundedDouble(0, 10, 1, 0, rand) * 2 - 10;
                nextStreamItem[0] = rand.nextDouble() * 2 - 1;
                nextStreamItem[1] = rand.nextDouble() * 2 - 1;

                String toWrite = String.format("%d, %d, %s\n", timestamp, id, Arrays.toString(nextStreamItem));
                myWriter.write(toWrite);

                id++;
                itemPerStamp--;
            }
            myWriter.close();
        }
        catch(Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    public static void createGroundTruth(String streamFileName, HashMap<String, Double[]> wordEmbeddings, Double threshold) throws Exception{
        LinkedList<Tuple3<Long,Integer,String>> records = new LinkedList<>();
        try (Stream<String> lines = Files.lines(Paths.get(pwd + "/src/main/resources/"+streamFileName+".txt"),Charset.defaultCharset())) {
            lines.map(l -> l.split(", "))
                    .forEach(l -> records.addFirst(new Tuple3<Long,Integer,String>(Long.parseLong(l[0]), Integer.parseInt(l[1]), l[2])));
        }
        try {
            FileWriter myWriter = new FileWriter(pwd + "/src/main/resources/"+streamFileName+"GroundTruth.txt");
            while (!records.isEmpty()) {
                Tuple3<Long,Integer,String> toCompare = records.poll();
                Double[] comEmb = wordEmbeddings.get(toCompare.f2);
                for(Tuple3<Long,Integer,String> r : records){
                    Double[] emb = wordEmbeddings.get(r.f2);
                    Double dist = CosineDistance(comEmb, emb);
//                    if(toCompare.f1 == 615 && r.f1==0){
//                        System.out.println(dist);
//                    }
                    if(dist < threshold){
//                        System.out.format("%d, %d, %f\n", toCompare.f1, r.f1, dist);
                        String toWrite = new Tuple2<Integer, Integer>(toCompare.f1, r.f1).toString().replaceAll("\\(","").replaceAll("\\)","") + "\n";
                        myWriter.write(toWrite);
                    }
                }
            }
            myWriter.close();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw e;
        }

    }

    public static void create2DGroundTruth(String streamFileName, Double threshold) throws Exception{
        LinkedList<Tuple3<Long,Integer,Double[]>> records = new LinkedList<>();
        try (Stream<String> lines = Files.lines(Paths.get(pwd + "/src/main/resources/"+streamFileName+".txt"),Charset.defaultCharset())) {
            lines.map(new Function<String, Tuple3<Long, Integer, Double[]>>() {
                @Override
                public Tuple3<Long, Integer, Double[]> apply(String s) {
                    String[] splitedString = s.split(", ", 3);
                    Double[] arr2D = new Double[2];

                    int counter = 0;
                    for(String d : splitedString[2].split(", ")){
                        arr2D[counter] = Double.parseDouble(d.replaceAll("[\\]\\[]", ""));
                        counter++;
                    }
                    return new Tuple3<Long,Integer,Double[]>(Long.parseLong(splitedString[0]), Integer.parseInt(splitedString[1]), arr2D);
                }
            })
                    .forEach(records::addFirst);
        }
        try {
            FileWriter myWriter = new FileWriter(pwd + "/src/main/resources/"+streamFileName+"GroundTruth"+threshold.toString().replace(".", "_")+".txt");
            while (!records.isEmpty()) {
                Tuple3<Long,Integer,Double[]> toCompare = records.poll();
                Double[] comEmb = toCompare.f2;
                for(Tuple3<Long,Integer,Double[]> r : records){
                    Double[] emb = r.f2;
                    Double dist = AngularDistance(comEmb, emb);
//                    if(toCompare.f1 == 992 && r.f1==244){
//                        System.out.println(dist);
//                    }
                    if(dist < threshold){
//                        System.out.format("%d, %d, %f\n", toCompare.f1, r.f1, dist);
                        String toWrite = new Tuple2<Integer, Integer>(toCompare.f1, r.f1).toString().replaceAll("\\(","").replaceAll("\\)","") + "\n";
                        myWriter.write(toWrite);
                    }
                }
            }
            myWriter.close();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    public static void create2WayGroundTruth(String streamLeft, String streamRight, Double threshold) throws Exception{
        LinkedList<Tuple3<Long,Integer,Double[]>> recordsLeft = new LinkedList<>();
        try (Stream<String> lines = Files.lines(Paths.get(pwd + "/src/main/resources/"+streamLeft+".txt"),Charset.defaultCharset())) {
            lines.map(new Function<String, Tuple3<Long, Integer, Double[]>>() {
                @Override
                public Tuple3<Long, Integer, Double[]> apply(String s) {
                    String[] splitedString = s.split(", ", 3);
                    Double[] arr2D = new Double[2];

                    int counter = 0;
                    for(String d : splitedString[2].split(", ")){
                        arr2D[counter] = Double.parseDouble(d.replaceAll("[\\]\\[]", ""));
                        counter++;
                    }
                    return new Tuple3<Long,Integer,Double[]>(Long.parseLong(splitedString[0]), Integer.parseInt(splitedString[1]), arr2D);
                }
            })
                    .forEach(recordsLeft::addFirst);
        }
        LinkedList<Tuple3<Long,Integer,Double[]>> recordsRight = new LinkedList<>();
        try (Stream<String> lines = Files.lines(Paths.get(pwd + "/src/main/resources/"+streamRight+".txt"),Charset.defaultCharset())) {
            lines.map(new Function<String, Tuple3<Long, Integer, Double[]>>() {
                @Override
                public Tuple3<Long, Integer, Double[]> apply(String s) {
                    String[] splitedString = s.split(", ", 3);
                    Double[] arr2D = new Double[2];

                    int counter = 0;
                    for(String d : splitedString[2].split(", ")){
                        arr2D[counter] = Double.parseDouble(d.replaceAll("[\\]\\[]", ""));
                        counter++;
                    }
                    return new Tuple3<Long,Integer,Double[]>(Long.parseLong(splitedString[0]), Integer.parseInt(splitedString[1]), arr2D);
                }
            })
                    .forEach(recordsRight::addFirst);
        }
        try {
            FileWriter myWriter = new FileWriter(pwd + "/src/main/resources/twoWayGroundTruth"+threshold.toString().replace(".", "_")+".txt");
            while (!recordsLeft.isEmpty()) {
                Tuple3<Long,Integer,Double[]> toCompare = recordsLeft.poll();
                Double[] comEmb = toCompare.f2;
                for(Tuple3<Long,Integer,Double[]> r : recordsRight){
                    Double[] emb = r.f2;
                    Double dist = AngularDistance(comEmb, emb);
                    if(dist < threshold){
                        String toWrite = new Tuple2<String, String>(toCompare.f1.toString() + "L", r.f1.toString() + "R").toString()
                                .replaceAll("\\(","").replaceAll("\\)","") + "\n";
                        myWriter.write(toWrite);
                    }
                }
            }
            myWriter.close();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw e;
        }


    }

    public static HashMap<String, Double[]> readEmbeddings(String minioObject, MinioConfiguration minio, Logger LOG)
            throws Exception{

        HashMap<String, Double[]> wordEmbeddings = new HashMap<>();

        LOG.info("readEmbeddings function");
        LOG.info("minio endpoint " + minio.getEndpoint());

        MinioClient minioClient =
                MinioClient.builder()
                        .endpoint(minio.getEndpoint())
                        .credentials(minio.getAccessKey(), minio.getSecretKey())
                        .build();

        LOG.info("Client created");

        // Read input file from Minio
        InputStream embeddingsFile = minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket("embeddings")
                        .object(minioObject)
                        .build());
        BufferedReader br = new BufferedReader( new InputStreamReader( embeddingsFile ) );
        br.readLine();

        LOG.info("Embeddings file read.");
        String line = "ok";
        try {
            while ((line = br.readLine()) != null) {
                String[] keyedEmbedding = line.split(" ", 2);
                wordEmbeddings.put(keyedEmbedding[0], arrayStringToDouble(keyedEmbedding[1].split(" "), 300));
            }
        }
        catch(Exception e){
            e.printStackTrace();
            String[] keyedEmbedding = line.split(" ", 2);
            System.out.println(keyedEmbedding[0]);
            System.out.println(Arrays.toString(keyedEmbedding[1].split(" ")));
            System.exit(-1);
        }

        LOG.info("Embeddings array created.");

        embeddingsFile.close();
        return wordEmbeddings;
    }


    public static HashMap<String, Double[]> readEmbeddingsLocal(String filename)
            throws Exception{

        HashMap<String, Double[]> wordEmbeddings = new HashMap<>();

        LOG.info("readEmbeddings function");


        LOG.info("Embeddings file read.");

        try (Stream<String> lines = Files.lines(Paths.get(filename))) {
            lines.skip(1).map(l -> l.split(" ",2))
                    .forEach(l -> wordEmbeddings.put(l[0], arrayStringToDouble(l[1].split(" "), 300)));
        }

        LOG.info("Embeddings array created.");

        return wordEmbeddings;
    }

    public static void cleanDataset(String embeddingsFile, String datasetFile) throws Exception {

        System.out.println("Let's read embeddings");
        Long startTime = System.currentTimeMillis();
        HashMap<String, Double[]> embeddings = readEmbeddingsLocal(embeddingsFile);
        System.out.println("Done with embeddings");
        System.out.format("It took %d seconds\n",(System.currentTimeMillis()-startTime)/1000);
        Set<String> keys = embeddings.keySet();

        System.out.println("Start reading the dataset.");
        Long startTimeData = System.currentTimeMillis();
        try (Stream<String> lines = Files.lines(Paths.get(datasetFile))) {
            try(PrintWriter pw = new PrintWriter(Files.newBufferedWriter(
                    Paths.get("/Users/gsiachamis/Dropbox/My Mac (Georgios’s MacBook Pro)/Documents/GitHub/filteredIMDB_test.txt")))) {
                lines
                        .skip(1)
                        .map(l -> l.split("\t")[2])
                        .map(l -> l.replaceAll("[^\\w\\s]",""))
                        .limit(1_000_000)
                        .filter(isContained(keys))
                        .forEach(pw::println);
            }
        }
        System.out.println("Read all (~10M) rows.");
        System.out.format("It took %d seconds\n",(System.currentTimeMillis()-startTimeData)/1000);

    }

    public static Double[] mapEmbeddings(String t, HashMap<String, Double[]> wordEmbeddings){
        Double[] embedding = new Double[300];
        Arrays.fill(embedding, 0.0);
        String[] sentence = t.split(" ");
        Set<String> keys = wordEmbeddings.keySet();
        int sum = 0;
//        System.out.println(Arrays.toString(sentence));
        try {
            for (String word : sentence) {
                if (keys.contains(word)) {
                    sum += 1;
                    Double[] tmp = wordEmbeddings.get(word);
                    for (int i = 0; i < 300; i++) {
                        embedding[i] += tmp[i];
                    }
                }
            }
            if (sum != 0) {
                for (int i = 0; i < 300; i++) {
                    embedding[i] = embedding[i] / sum;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(Arrays.toString(sentence));
            System.out.println(Arrays.toString(embedding));
            System.exit(-1);
        }
        return embedding;
    }

    public static void createEmbeddingsFiles(String embeddingsFile, String datasetFile) throws Exception {

        System.out.println("Let's read embeddings");
        Long startTime = System.currentTimeMillis();
        HashMap<String, Double[]> embeddings = readEmbeddingsLocal(embeddingsFile);
        System.out.println("Done with embeddings");
        System.out.format("It took %d seconds\n",(System.currentTimeMillis()-startTime)/1000);
        Set<String> keys = embeddings.keySet();

        System.out.println("Start reading the dataset.");
        Long startTimeData = System.currentTimeMillis();
        try (Stream<String> lines = Files.lines(Paths.get(datasetFile))) {

            try(PrintWriter pw = new PrintWriter(Files.newBufferedWriter(
                    Paths.get("/Users/gsiachamis/Dropbox/My Mac (Georgios’s MacBook Pro)/Documents/GitHub/embeddingsIMDB_100K.txt")))) {
                lines
                        .skip(1)
                        .map(l -> l.split("\t")[2])
                        .map(l -> l.replaceAll("[^\\w\\s]",""))
                        .limit(100_000)
                        .filter(isContained(keys))
                        .map(p -> mapEmbeddings(p, embeddings))
                        .map(Arrays::toString)
                        .forEach(pw::println);
            }
        }
        System.out.println("Read ~100K rows.");
        System.out.format("It took %d seconds\n",(System.currentTimeMillis()-startTimeData)/1000);

    }

//    public void cleanAmazonDataset(String embeddingsFile, String datasetFile) throws Exception{
//        System.out.println("Let's read embeddings");
//        Long startTime = System.currentTimeMillis();
//        HashMap<String, Double[]> embeddings = readEmbeddingsLocal(embeddingsFile);
//        System.out.println("Done with embeddings");
//        System.out.format("It took %d seconds\n",(System.currentTimeMillis()-startTime)/1000);
//        Set<String> keys = embeddings.keySet();
//
//        System.out.println("Start reading the dataset.");
//        Long startTimeData = System.currentTimeMillis();
//        try (Stream<String> lines = Files.lines(Paths.get(datasetFile))) {
//            try(PrintWriter pw = new PrintWriter(Files.newBufferedWriter(
//                    Paths.get("/Users/gsiachamis/Dropbox/My Mac (Georgios’s MacBook Pro)/Documents/GitHub/filteredAmazon_100000_all.txt")))) {
//                lines
//                        .skip(1)
//                        .filter()
//                        .limit(100_000)
//                        .filter(isContained(keys))
//                        .forEach(pw::println);
//            }
//        }
//        System.out.println("Read all (~10M) rows.");
//        System.out.format("It took %d seconds\n",(System.currentTimeMillis()-startTimeData)/1000);
//    }

    public static void main(String[] args) throws Exception{
        createEmbeddingsFiles(
                "/Users/gsiachamis/PycharmProjects/clustering_embeddings/wiki-news-300d-1M.vec",
                "/Users/gsiachamis/Dropbox/My Mac (Georgios’s MacBook Pro)/Documents/PhD/Datasets/SSJ/IMDB-movies/title.basics.tsv"
        );
    }


}
