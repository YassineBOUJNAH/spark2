import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.*;
import scala.Tuple2;
import twitter4j.Status;

import java.util.Arrays;


public class twitter {
    public static void main(String[] args) {

        final String consumerKey = "kzgrPSagJviBsDRGTrun6Zx3d";
        final String consumerSecret = "2r28T0g1NTpJEkn6CR9AkspqkhkFFxB444gUU14PJkSiKCwbG4";
        final String accessToken = "1635592039-A9d59qfaNjAxwNnj2u6LBbhlv5h5lBcu6GDRqEl";
        final String accessTokenSecret = "Lcd1YNIgS1IGnlF9cwZlWkaMVVuLgqwEatHgLBuGywSf3";

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkTwitterCountHashTag");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(4000));

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);
        JavaDStream<String> data=twitterStream.map(status->status.getText());
        JavaDStream<String> tweetwords=data.flatMap(tweetText -> Arrays.asList(tweetText.split(" ")).iterator());
        JavaDStream<String> hashtags=tweetwords.filter(word ->word.startsWith("#"));
        JavaPairDStream<String, Integer> hashtagKeyValues=hashtags.mapToPair(hash-> new Tuple2<>(hash, 1))
                .reduceByKey((a, b) -> a + b);
        hashtagKeyValues.print();

        data.foreachRDD(rdd->{
            rdd.saveAsTextFile("C:\\Users\\boujnah\\Desktop\\data\\wordcount\\output13");
        });

        data.persist();

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
