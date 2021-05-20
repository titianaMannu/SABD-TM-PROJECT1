package query;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

import utils.SomministrationSummary;
import utils.VaccinationCenter;

import java.time.LocalDate;
import java.time.YearMonth;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.math3.util.Precision.round;


public class Query1 {
    private final static String pathToFile = "data/punti-somministrazione-tipologia.csv";
    private final static String pathToFile2 = "data/somministrazioni-vaccini-summary-latest.csv";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("VaccinationQuery");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> textFile = sc.textFile(pathToFile);
        JavaRDD<VaccinationCenter> points = textFile.map(line -> VaccinationCenter.parse(line)); //.distinct();

        // to obtain (area_code, (1, area_name)) we will use area_name later
        JavaPairRDD<String, Tuple2<Integer, String>> pairs = points.mapToPair(point ->
                new Tuple2<>(point.getAreaCode(), new Tuple2<>(1, point.getAreaName())));

        // to obtain how many vaccination points in every area: (area_code, (total, area_name) )
        JavaPairRDD<String, Tuple2<Integer, String>> summary = pairs.reduceByKey(
                (x, y) -> new Tuple2<>(x._1() + y._1(), x._2()));

        //for debug ... show intermediary results
        List<Tuple2<String, Tuple2<Integer, String>>> results = summary.collect();
        for (Tuple2<String, Tuple2<Integer, String>> o : results) {
            System.out.println(o);
        }

        //second part ------------------------------------------------------------

        JavaRDD<String> textFile2 = sc.textFile(pathToFile2);
        JavaRDD<SomministrationSummary> somministrationSummaryJavaRDD = textFile2.map(line -> SomministrationSummary.CSVParser(line))
                .filter(obj -> obj != null && obj.getSomministrationDate().isAfter(LocalDate.parse("2020-12-31")));

        JavaPairRDD<Tuple2<String, YearMonth>, Tuple2<Integer, Integer>> summaryDayRdd = somministrationSummaryJavaRDD.mapToPair(obj -> new Tuple2<>(
                new Tuple2<>(obj.getArea(), YearMonth.from(obj.getSomministrationDate())),
                new Tuple2<>(obj.getTotal(), 1)
        )).reduceByKey((x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2()));


        JavaPairRDD<String, Tuple3<YearMonth, Integer, Integer>> t3_summaryDay = summaryDayRdd.mapToPair(
                x -> new Tuple2(x._1()._1(),
                        new Tuple3(x._1()._2(), x._2()._1(), x._2()._2()))
        );


      /*  List<Tuple2<String, Tuple3<YearMonth, Integer, Integer>>> l = t3_summaryDay.collect();
        for (Tuple2<String, Tuple3<YearMonth, Integer, Integer>> e : l) {
            System.out.println(e);
        }*/

        JavaPairRDD<String, Tuple2<Tuple3<YearMonth, Integer, Integer>,
                Tuple2<Integer, String>>> joined = t3_summaryDay.join(summary);

       /* List<Tuple2<String, Tuple2<Tuple3<YearMonth, Integer, Integer>,
                Tuple2<Integer, String>>>> finalList = joined.collect();

        for (Tuple2<String, Tuple2<Tuple3<YearMonth, Integer, Integer>,
                Tuple2<Integer, String>>> x :
                finalList) {
            System.out.println(x);
        }
*/

        JavaRDD<Tuple3<String, YearMonth, Double>> average = joined.map(x ->
                new Tuple3<>(
                        x._2._2._2(),
                        x._2._1._1(),
                        round(x._2._1._2() / Double.valueOf(x._2._1._3() * x._2._2._1()), 3)
                ));

        List<Tuple3<String, YearMonth, Double>> ff = average.collect();
        for (Tuple3<String, YearMonth, Double> elem : ff) {
            System.out.println(elem);
        }

        try {
            TimeUnit.MINUTES.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        sc.stop();
    }


}
