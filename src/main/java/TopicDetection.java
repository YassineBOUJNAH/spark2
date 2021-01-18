import io.netty.util.HashingStrategy;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;

public class TopicDetection {

    private static HashSet<String> politicsSet = new HashSet<>();
    private static HashSet<String> socialSet = new HashSet<>();
    private static HashSet<String> economicSet = new HashSet<>();

    public static void main(String[] args) {

        String uri1 = args[0];
        String uri2 = args[1];
        String uri3 = args[2];

        SparkConf conf = new SparkConf().setAppName("TopicDetection").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> politicFile1 = context.textFile(uri1);
        politicFile1.foreach(s -> politicsSet.add(s.toUpperCase()));

        JavaRDD<String> socialFile2 = context.textFile(uri2);
        socialFile2.foreach(s -> socialSet.add(s.toUpperCase()));

        JavaRDD<String> economicFile3 = context.textFile(uri3);
        economicFile3.foreach(s -> economicSet.add(s.toUpperCase()));

        JavaRDD<String> textFile4 = context.textFile(uri3);


        JavaRDD<String> speechFile = context.textFile(args[3]);
            JavaPairRDD<String, Integer> topic = speechFile
                    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                    .mapToPair(word -> {
                        if(politicsSet.contains(word.toUpperCase()))
                            return new Tuple2<>("politics_word", 1);
                        if(socialSet.contains(word.toUpperCase()))
                            return new Tuple2<>("social_word", 1);
                        if(economicSet.contains(word.toUpperCase()))
                            return new Tuple2<>("econimics_word", 1);
                        return new Tuple2<>("",0);
                    })
                    .reduceByKey((a, b) -> a + b);
        topic.saveAsTextFile(args[4]);
    }
}

