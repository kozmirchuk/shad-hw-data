import com.google.common.base.Optional;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;


public final class KafkaStreaming {

   // private static final AtomicLong total = new AtomicLong(0);

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf().setAppName("KozmirchukKafkaStreaming");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(15));

        jssc.checkpoint("hdfs:/user/akozmirchuk/hw3/checkpoint");


        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put("bigdatashad-csc", 1);

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, "hadoop2-10:2181", "Kozmirchuk-group", topicMap);



        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

        JavaDStream<String> failed = lines.filter(s -> !s.split(" ")[8].equals("200"));

        JavaDStream<Long> failedLast60 = failed.countByWindow(Durations.seconds(60), Durations.seconds(15));

        JavaDStream<Long> failedCounted = failed.count();

        JavaDStream<String> failedToPrint = failedCounted.map(cnt -> "15_second_count=" + cnt);
        JavaDStream<String> failed60ToPrint = failedLast60.map(cnt -> "60_second_count=" + cnt);

        JavaPairDStream<String, Long> total = failedCounted
                .mapToPair(aLong -> new Tuple2<>("total", aLong))
                .updateStateByKey((longs, state) -> {

                    long current = state.or(0L);

                    if (longs.size() != 1)
                        throw new RuntimeException("OLOLOLOLOLOL");

                    return Optional.of(current + longs.get(0));
                });

        JavaDStream<String> totalToPrint = total.map(tuple -> "total_count=" + tuple._2);

        JavaDStream<String> union = failedToPrint.union(failed60ToPrint).union(totalToPrint);

        JavaDStream<String> result = union.reduce((f,s) -> f + "; " + s);


        result.print();


        jssc.start();
        jssc.awaitTermination();
    }


}
