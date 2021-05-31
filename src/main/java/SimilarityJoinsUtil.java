import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import scala.Int;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public class SimilarityJoinsUtil {

    static String pwd = Paths.get("").toAbsolutePath().toString();

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

    static public double nextSkewedBoundedDouble(double min, double max, double skew, double bias, Random rand) {
        double range = max - min;
        double mid = min + range / 2.0;
        double unitGaussian = rand.nextGaussian();
        double biasFactor = Math.exp(bias);
        double retval = mid+(range*(biasFactor/(biasFactor+Math.exp(-unitGaussian/skew))-0.5));
        return retval;
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

    public static HashMap<String, Double[]> readEmbeddings(String file4WE) throws Exception{
        HashMap<String, Double[]> wordEmbeddings = new HashMap<>();
        try (Stream<String> lines = Files.lines(Paths.get(pwd + "/src/main/resources/"+file4WE),Charset.defaultCharset()).skip(1)) {
            lines.map(l -> l.split(" ",2))
                    .forEach(l -> wordEmbeddings.put(l[0], arrayStringToDouble(l[1].split(" "), 300)));
        }
        return wordEmbeddings;
    }

    public static void main(String[] args) throws Exception{

//        create2DArrayStream(1000);
//        create2DGroundTruth("1K_2D_Array_Stream_v2", 0.05);
//        HashMap<Integer, Double[]> cent = SimilarityJoinsUtil.RandomCentroids(10, 2);
//        for(Integer k : cent.keySet()){
//            System.out.println(Arrays.toString(cent.get(k)));
//        }
//        System.out.println(CosineDistance(cent.get(3), cent.get(6)));
        Double[] vectorA;
        Double[] vectorB;
//
        vectorA = new Double[]{0.0, 1.0};
        vectorB = new Double[]{0.727, 1.0};
        System.out.println(AngularDistance(vectorA, vectorB));
//
//        vectorA = new Double[]{0.0, 1.0};
//        vectorB = new Double[]{0.44407, 0.89599};
//        System.out.println(AngularDistance(vectorA, vectorB));
//
//        vectorA = new Double[]{-0.46788, 0.88379};
//        vectorB = new Double[]{-0.31649, 0.94859};
//        System.out.println(AngularDistance(vectorA, vectorB));
    }


}
