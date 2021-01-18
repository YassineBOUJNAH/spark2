//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import scala.Tuple2;
//
//import java.util.Arrays;
//
//public class CaracterCount {
//    public static void main(String[] args) {
//        SparkConf conf = new SparkConf().setAppName("carcterCount").setMaster("local");
//        JavaSparkContext context = new JavaSparkContext(conf);
//
//        JavaRDD<String> textFile = context.textFile(args[0]);
//        JavaPairRDD<String, Integer> counts = textFile
//                .flatMap(s -> Arrays.asList(s.split("")).iterator())
//                .mapToPair(caract -> new Tuple2<>(caract.toUpperCase(), 1))
//                .reduceByKey((a, b) -> a + b);
//        counts.saveAsTextFile(args[1]);
//    }
//}
