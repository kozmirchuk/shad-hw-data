import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;

/**
 * Created by antonkozmirchuk on 28/03/17.
 */
public class Application {

    public static void main(String[] args) throws IOException {
        SparkConf spark = new SparkConf().setAppName("Kozmirchuk_HW1");

        JavaSparkContext sc = new JavaSparkContext(spark);

        FileSystem fs = FileSystem.get(sc.hadoopConfiguration());


        //JavaRDD<String> logs = sc.textFile("hdfs:/user/sandello/logs/*");

        JavaRDD<String> logs = sc.textFile("hdfs:/user/akozmirchuk/test.txt");

        /* Group 1 */
        /*long hits = logs.filter( s -> s.split(" ")[8].equals("200")).count();

        long uniqueUsers = logs
                .map(it -> {

            String[] splitted = it.split(" ");
            return new Tuple2<>(splitted[8], splitted[0]);

        })
                .filter( tuple -> tuple._1.equals("200"))
                .map(tuple -> tuple._2)
                .distinct()
                .count();


        JavaPairRDD<Integer, String> pages = logs
                .map(it -> {
                    String[] splitted = it.split(" ");
                    return new Tuple2<>(splitted[8], splitted[10]);
                })
                .filter( tuple -> tuple._1.equals("200"))
                .mapToPair(tuple -> new Tuple2<>(tuple._2, 1))
                .reduceByKey( (a, b) -> a + b)
                .mapToPair(pair -> new Tuple2<>(pair._2, pair._1));



        List<Tuple2<Integer, String>> topPages = pages.top(10, new Tuple2Comparator());
*/

        /* Group 2 */

        /* Group 3 */

        JavaRDD<String> countries = sc.textFile("hdfs:/user/sandello/dicts/IP2LOCATION-LITE-DB1.CSV");

        JavaPairRDD<String, Tuple2<Integer, Integer>> ipPerCountry = countries
                .mapToPair(line -> {
            String[] splitted = line.split(",");
            String country = splitted[3].replaceAll(",", "");
            Integer start = Integer.valueOf(splitted[0].replaceAll(",", ""));
            Integer end = Integer.valueOf(splitted[1].replaceAll(",", ""));
            return new Tuple2<>(country, new Tuple2<>(start, end));
        });


        JavaPairRDD<String, Integer> userCountries = logs
                .map(it -> {
            String[] splitted = it.split(" ");
            return new Tuple2<>(splitted[8], splitted[0]);
        })
                .filter( tuple -> tuple._1.equals("200"))
                .map(tuple -> tuple._2)
                .map(Application::convertIp)
                .sortBy(i -> i, true, 10)
                .flatMap(i -> ipPerCountry.filter(tuple -> {
                    Integer start = tuple._2._1;
                    Integer end = tuple._2._2;
                    return start >= i && end <= i;
                }).map(tuple -> tuple._1).top(1))
                .mapToPair(country -> new Tuple2<>(country, 1))
                .reduceByKey( (a, b) -> a + b);





        userCountries.saveAsTextFile("hdfs:/user/akozmirchuk/hw1/countries.txt");


        FSDataOutputStream output = fs.create(new Path("/user/akozmirchuk/hw1/results.txt"));

       /* StringBuilder builder = new StringBuilder();

        builder.append(hits);
        builder.append("\n");
        builder.append(uniqueUsers);
        builder.append("\n");

        for(Tuple2<Integer, String> t : topPages) {
            builder.append(t._1);
            builder.append(" ");
            builder.append(t._2);
            builder.append("\n");
        }


        try(BufferedOutputStream os = new BufferedOutputStream(output)) {

            os.write(builder.toString().getBytes());
        }

        sc.close();
*/

    }

    public static int convertIp(String ip) {

        String[] splitted = ip.split("\\.");

        int byte0 = Integer.valueOf(splitted[0]);
        int byte1 = Integer.valueOf(splitted[1]);
        int byte2 = Integer.valueOf(splitted[2]);
        int byte3 = Integer.valueOf(splitted[3]);

        return byte0 << 24 | byte1 << 16 | byte2 << 8 | byte3 << 0;

    }

}
