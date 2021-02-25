import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class SimilarityJoinsUtil {

    static String pwd = Paths.get("").toAbsolutePath().toString();

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
        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    public static Double CosineDistance(Double[] vectorA, Double[] vectorB){
        return 1 - CosineSimilarity(vectorA, vectorB);
    }

    public static Double EuclideanDistance(Double[] vectorA, Double[] vectorB){
        double[] vecA = ArrayUtils.toPrimitive(vectorA);
        double[] vecB = ArrayUtils.toPrimitive(vectorB);
        return new org.apache.commons.math3.ml.distance.EuclideanDistance().compute(vecA, vecB);
    }

    public static Double[] arrayStringToDouble(String[] stringArray){
        Double[] doubleArray = new Double[300];
        int counter = 0;
        for(String s : stringArray) {
            doubleArray[counter] = Double.parseDouble(s);
            counter++;
        }
        return doubleArray;
    }

    public static void createStreamFile(HashMap<String, Double[]> embeddings, int N) throws Exception{
        try {
            File myFile = new File(pwd + "/src/main/resources/wordStream.txt");
            if (myFile.createNewFile()) {
                System.out.println("File created: " + myFile.getName());
            } else {
                System.out.println("File already exists.");
            }
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            throw e;
        }

        Set<String> setKeys = embeddings.keySet();
        int arrSize = setKeys.size();
        String[] keys = setKeys.toArray(new String[arrSize]);

        try {
            FileWriter myWriter = new FileWriter(pwd + "/src/main/resources/wordStream.txt");
            Random rand = new Random(1000);
            int timestamp = 0;
            int id = 0;
            int itemPerStamp = 10;
            for(int i = 0; i < N; i++){
                if(itemPerStamp == 0){
                    itemPerStamp = 1 + rand.nextInt(100);
                    timestamp++;
                }

                String nextStreamItem = keys[1 + rand.nextInt(1000)];
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

    public static void createGroundTruth(String streamFileName, HashMap<String, Double[]> wordEmbeddings, Double threshold) throws Exception{
        LinkedList<Tuple3<Long,Integer,String>> records = new LinkedList<>();
        try (Stream<String> lines = Files.lines(Paths.get(pwd + "/src/main/resources/"+streamFileName),Charset.defaultCharset())) {
            lines.map(l -> l.split(", "))
                    .forEach(l -> records.addFirst(new Tuple3<Long,Integer,String>(Long.parseLong(l[0]), Integer.parseInt(l[1]), l[2])));
        }
        try {
            FileWriter myWriter = new FileWriter(pwd + "/src/main/resources/dummyStreamGroundTruth.txt");
            while (!records.isEmpty()) {
                Tuple3<Long,Integer,String> toCompare = records.poll();
                Double[] comEmb = wordEmbeddings.get(toCompare.f2);
                for(Tuple3<Long,Integer,String> r : records){
                    Double[] emb = wordEmbeddings.get(r.f2);
                    Double dist = CosineDistance(comEmb, emb);
                    if(toCompare.f1 == 615 && r.f1==0){
                        System.out.println(dist);
                    }
                    if(dist < threshold){
                        System.out.format("%d, %d, %f\n", toCompare.f1, r.f1, dist);
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

    public static HashMap<String, Double[]> readEmbeddings(String file4WE) throws Exception{
        HashMap<String, Double[]> wordEmbeddings = new HashMap<>();
        try (Stream<String> lines = Files.lines(Paths.get(pwd + "/src/main/resources/"+file4WE),Charset.defaultCharset()).skip(1)) {
            lines.map(l -> l.split(" ",2))
                    .forEach(l -> wordEmbeddings.put(l[0], arrayStringToDouble(l[1].split(" "))));
        }
        return wordEmbeddings;
    }

    public static void main(String[] args) throws Exception{

        HashMap<String, Double[]> wordEmbeddings = new HashMap<>();
        wordEmbeddings = readEmbeddings("wiki-news-300d-1K.vec");

        createGroundTruth("dummyStream.txt", wordEmbeddings, 0.3);

    }


}
