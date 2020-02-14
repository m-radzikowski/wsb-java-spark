import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class App {

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        String path = App.class.getResource("king_james_bible.txt").getPath();

        JavaRDD<String> lines = ctx.textFile(path, 1);

        JavaPairRDD<Integer, String> counts = lines
                .flatMap(s -> Arrays.asList(s.split(" ")))
                .map(s -> s.replaceAll("[^\\p{IsAlphabetic}]", "").toLowerCase())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .mapToPair(tuple -> tuple.swap())
                .sortByKey(false);

        List<Tuple2<Integer, String>> output = counts.take(100);
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        ctx.stop();
    }

}
