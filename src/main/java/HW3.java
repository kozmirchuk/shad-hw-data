import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedOutputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Created by antonkozmirchuk on 25/04/17.
 */
public class HW3 {

    public static void main(String[] args) throws Exception{
        SparkConf spark = new SparkConf().setAppName("Kozmirchuk_HW1");

        JavaSparkContext sc = new JavaSparkContext(spark);

        FileSystem fs = FileSystem.get(sc.hadoopConfiguration());

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate todayLocalData = LocalDate.now();

        JavaRDD<String> today = sc.textFile("hdfs:/user/sandello/logs/access.log."
                + dtf.format(todayLocalData));
        JavaRDD<String> todayMinus1 = sc.textFile("hdfs:/user/sandello/logs/access.log."
                + dtf.format(todayLocalData.minusDays(1)));
        JavaRDD<String> todayMinus2 = sc.textFile("hdfs:/user/sandello/logs/access.log."
                + dtf.format(todayLocalData.minusDays(2)));


        JavaRDD<String> threeDays = extractLikedProfiles(today)
                .union(extractLikedProfiles(todayMinus1))
                .union(extractLikedProfiles(todayMinus2))
                .distinct();


        FSDataOutputStream output = fs.create(new Path("/user/akozmirchuk/hw3/" + dtf.format(todayLocalData) + ".txt"));

        StringBuilder builder = new StringBuilder();

        builder.append("profile_liked_three_days=").append(threeDays.count());


        try(BufferedOutputStream os = new BufferedOutputStream(output)) {

            os.write(builder.toString().getBytes());
        }

        sc.close();


    }

    private static JavaRDD<String> extractLikedProfiles(JavaRDD<String> rdd) {

        return rdd
                .filter(s -> s.split(" ")[8].equals("200"))
                .map(s -> s.split(" ")[6])
                .filter(s -> s.endsWith("like=1"))
                .distinct();
    }

}
