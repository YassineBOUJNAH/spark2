import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.util.Arrays;
import java.util.HashSet;


public class twitter2 {

    private static HashSet<String> positiveSet = new HashSet<>();
    private static HashSet<String> negativeSet = new HashSet<>();
    private static HashSet<String> paysSet = new HashSet<>();


    public static void main(String[] args) {


        String uri1 = args[0];
        String uri2 = args[1];
        String uri3 = args[3];

        SparkConf conf = new SparkConf().setAppName("sentiment2").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaStreamingContext jssc = new JavaStreamingContext(context, new Duration(30000));
        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc,null,new String[] {"nike"});


        JavaRDD<String> positiveFile = context.textFile(uri1);
        positiveFile.foreach(s -> positiveSet.add(s.toUpperCase()));

        JavaRDD<String> negativeFile = context.textFile(uri2);
        negativeFile.foreach(s -> negativeSet.add(s.toUpperCase()));

        JavaRDD<String> paysFile = context.textFile(uri3);
        negativeFile.foreach(s -> paysSet.add(s.toUpperCase()));


        final String consumerKey = "kzgrPSagJviBsDRGTrun6Zx3d";
        final String consumerSecret = "2r28T0g1NTpJEkn6CR9AkspqkhkFFxB444gUU14PJkSiKCwbG4";
        final String accessToken = "1635592039-A9d59qfaNjAxwNnj2u6LBbhlv5h5lBcu6GDRqEl";
        final String accessTokenSecret = "Lcd1YNIgS1IGnlF9cwZlWkaMVVuLgqwEatHgLBuGywSf3";


        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);
        String twittePlace = "";

        JavaDStream<String> data=twitterStream.map(status->status.getText());
        JavaDStream<String> tweetwords=data.flatMap(tweetText -> Arrays.asList(tweetText.split(" ")).iterator());


        JavaPairDStream<String, Integer> tweetSentiment = tweetwords.mapToPair(word -> {
            if(positiveSet.contains(word.toUpperCase()))
                return new Tuple2<>("positive_word" + twittePlace, 1);
            if(negativeSet.contains(word.toUpperCase()))
                return new Tuple2<>("negative_word" + twittePlace, 1);
            return new Tuple2<>("",0);
        }).reduceByKey((a, b) -> a + b);

        tweetSentiment.print();

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
