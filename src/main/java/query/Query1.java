package query;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;
import scala.Tuple3;
import utils.ExporterToCSV;
import utils.beans.SomministrationSummary;
import utils.beans.VaccinationCenter;
import utils.comparators.Tuple3Comparator;
import utils.enums.Constants;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.List;
import java.util.concurrent.TimeUnit;


import static org.apache.commons.math3.util.Precision.round;


public class Query1 {
    private final static String pathToFile = Constants.PATHQ1_CENTRI.getString();
    private final static String pathToFile2 = Constants.PATHQ1_SUMMARY.getString();
    private boolean isDebugMode;
    private long lastExecutionTime = 0;

    public Query1(boolean isDebugMode) {
        this.isDebugMode = isDebugMode;
    }

    public long getLastExecutionTime() {
        return lastExecutionTime;
    }

    public void executeQuery(JavaSparkContext sc) {
        Instant start = Instant.now();
        JavaRDD<String> textFile = sc.textFile(pathToFile);
        JavaRDD<VaccinationCenter> vaccinationCenterJavaRDD = textFile.map(line -> VaccinationCenter.parse(line)); //.distinct();

        // to obtain (area_code, (1, area_name)) we will use area_name later
        JavaPairRDD<String, Tuple2<Integer, String>> centrePerArea = vaccinationCenterJavaRDD.mapToPair(point ->
                new Tuple2<>(point.getAreaCode(), new Tuple2<>(1, point.getAreaName())));

        // to obtain how many vaccination points in every area: (area_code, (total, area_name) )
        JavaPairRDD<String, Tuple2<Integer, String>> summaryCentrePerArea = centrePerArea.reduceByKey(
                (x, y) -> new Tuple2<>(x._1() + y._1(), x._2()));


        if (isDebugMode) {
            System.out.println("**************************** Vaccination centre per area ***************************");
            //for debug ... show intermediary results
            List<Tuple2<String, Tuple2<Integer, String>>> results = summaryCentrePerArea.collect();
            for (Tuple2<String, Tuple2<Integer, String>> o : results) {
                System.out.println(o);
            }
        }

        //second part ------------------------------------------------------------

        JavaRDD<String> textFile2 = sc.textFile(pathToFile2);
        JavaRDD<SomministrationSummary> somministrationSummaryJavaRDD = textFile2.map(line -> SomministrationSummary.CSVParser(line))
                .filter(obj -> obj != null && obj.getSomministrationDate().isAfter(LocalDate.parse("2020-12-31")));

        // obtain how many vaccinations and how many active days in terms of vaccination per Area and Month
        // counting the vaccination days is done in a similar way of a wordCount
        // Not divide each elements in the summation for the same number (vaccination days) but do this one time at the end
        JavaPairRDD<Tuple2<String, YearMonth>, Tuple2<Integer, Integer>> summaryDayRdd = somministrationSummaryJavaRDD.mapToPair(obj -> new Tuple2<>(
                new Tuple2<>(obj.getArea(), YearMonth.from(obj.getSomministrationDate())),
                new Tuple2<>(obj.getTotal(), 1)
        )).reduceByKey((x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2()));


        //use Area CODE as key to perform join next
        JavaPairRDD<String, Tuple3<YearMonth, Integer, Integer>> MonthVaccinationsDaysPerArea = summaryDayRdd.mapToPair(
                x -> new Tuple2(x._1._1(),
                        new Tuple3(x._1._2(), x._2._1(), x._2._2()))
        );


        //<Area_CODE, ((month, Vaccinations, DaysOfVaccination), (numOfVaccinationCentre, Area_NAME)>
        JavaPairRDD<String, Tuple2<Tuple3<YearMonth, Integer, Integer>,
                Tuple2<Integer, String>>> joined = MonthVaccinationsDaysPerArea.join(summaryCentrePerArea);

        if (isDebugMode) {
            System.out.println("**************************** Area_code, <Month, vaccinations, vaccination_days>, <total_centre, area_name> ***************************");
            List<Tuple2<String, Tuple2<Tuple3<YearMonth, Integer, Integer>,
                    Tuple2<Integer, String>>>> finalList = joined.collect();

            for (Tuple2<String, Tuple2<Tuple3<YearMonth, Integer, Integer>,
                    Tuple2<Integer, String>>> x :
                    finalList) {
                System.out.println(x);
            }
        }


        JavaPairRDD<javaslang.Tuple3<YearMonth, String, Double>, Integer> average = joined.mapToPair(x ->
                new Tuple2<>(
                        new javaslang.Tuple3<>(
                                x._2._1._1(), // Month
                                x._2._2._2(), //Area NAME
                                // VaccinationOfMonth / (vaccination_DAYS * vaccination_CENTRE)
                                round(x._2._1._2() / (double) (x._2._1._3() * x._2._2._1()), 3)),
                        1 //need for sorting later on
                ));

        //sorting
        average = average.sortByKey(new Tuple3Comparator(), true);


        if (isDebugMode) {
            List<Tuple2<javaslang.Tuple3<YearMonth, String, Double>, Integer>> ff = average.collect();
            for (Tuple2<javaslang.Tuple3<YearMonth, String, Double>, Integer> elem : ff) {
                System.out.println(elem._1());
            }
        }

        JavaRDD<Row> rowRDD = average.map(tuple -> RowFactory.create(tuple._1._1().toString(), tuple._1._2(), tuple._1._3().toString()));
        // The schema is encoded in a string
        String schemaString = Constants.Q1_SCHEMA.getString();
        ExporterToCSV exporterToCSV = new ExporterToCSV(schemaString, Constants.OUTPUT_PATH_Q1.getString());
        exporterToCSV.generateCSV(sc, rowRDD);


        Instant end = Instant.now();
        this.lastExecutionTime = Duration.between(start, end).toMillis();

        if (isDebugMode) {
            try {
                TimeUnit.MINUTES.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }


    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster(Constants.MASTER_URL.getString())
                .setAppName("VaccinationQuery1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        Query1 q1 = new Query1(false);
        q1.executeQuery(sc);
        System.out.println("execution time for query 2: " + q1.getLastExecutionTime());

        sc.stop();
    }
}
