import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashSet;

public class sentiment {

    private static HashSet<String> positiveSet = new HashSet<>();
    private static HashSet<String> negativeSet = new HashSet<>();

    public static void main(String[] args) {

        String uri1 = args[0];
        String uri2 = args[1];

        SparkConf conf = new SparkConf().setAppName("sentiment").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> positiveFile = context.textFile(uri1);
        positiveFile.foreach(s -> positiveSet.add(s.toUpperCase()));

        JavaRDD<String> negativeFile = context.textFile(uri2);
        negativeFile.foreach(s -> negativeSet.add(s.toUpperCase()));

        JavaRDD<String> speechFile = context.textFile(args[2]);
            JavaPairRDD<String, Integer> topic = speechFile
                    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                    .mapToPair(word -> {
                        if(positiveSet.contains(word.toUpperCase()))
                            return new Tuple2<>("positive_word", 1);
                        if(negativeSet.contains(word.toUpperCase()))
                            return new Tuple2<>("negative_word", 1);
                        return new Tuple2<>("",0);
                    })
                    .reduceByKey((a, b) -> a + b);
        topic.saveAsTextFile(args[3]);
    }
}

