package query;


import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple3;
import utils.AgeCategory;
import utils.MyIterable;
import utils.SomministrationLatest;
import utils.Tuple2Comparator;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.abs;
import static org.apache.commons.math3.util.Precision.round;

public class Query2 {
    private final static String pathToFile = "data/somministrazioni-vaccini-latest.csv";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("VaccinationQuery");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> textFile = sc.textFile(pathToFile);
        JavaRDD<SomministrationLatest> VaccinationLatest = textFile.map(SomministrationLatest::CSVParser)
                .filter(obj -> obj != null && obj.getDate().isAfter(LocalDate.parse("2021-01-31")) && obj.getAgeCategory() != null && obj.getFemale_vaccination() > 0);

        //we have multiple lines with same date, area, and age_category but different vaccination brand -> total per day is the sum of them
        // ( date, area, age_category), vaccination_total_female
        JavaPairRDD<Tuple3<LocalDate, String, AgeCategory>, Integer> VaccinationPerDayAndAreaAndCategory = VaccinationLatest.mapToPair(my_obj -> new Tuple2<>(
                new Tuple3<>(my_obj.getDate(), my_obj.getArea(), my_obj.getAgeCategory()),
                my_obj.getFemale_vaccination()
        )).reduceByKey(Integer::sum);


        // <(month, region, age), MyIterable -> List[(complete_date, female_vaccinations)]>
        //use of custom Iterable to implements custom functions and a serializable Object
        JavaPairRDD<Tuple3<YearMonth, String, AgeCategory>, MyIterable> vaccinationPerMonthAreaCategory = VaccinationPerDayAndAreaAndCategory
                .mapToPair(
                        v -> new Tuple2<>(
                                new Tuple3<>(YearMonth.from(v._1._1()), v._1._2(), v._1._3()),
                                new MyIterable(new Tuple2<>(v._1._1(), v._2()))
                        )
                ).reduceByKey(MyIterable::addAll) // pattern to avoid groupByKey and reduce shuffled data
                .filter(o -> o._2().getList().size() > 1);  // delete instances with just one day of vaccinations


        // do predictions on the total vaccinations (females) based on the data of the previous month in a specific region and age
        JavaPairRDD<Tuple3<YearMonth, String, AgeCategory>, Tuple2<LocalDate, Double>> predictedVaccinations =
                vaccinationPerMonthAreaCategory.mapValues(new Predictor());


        /*
         * First of all perform groupByKey using a pattern map + reduceByKey.
         * I have grouped on month and category to obtain a list of tuple2<region, predicted_val>
         * After executes sort operation in a mapValues. Sorting operation is pretty fast because every list has size <= 21 elements
         * one per Italian region.
         * Final use Sublist to keep the topN elements, with N=5.
         * <(firstDayOfMonth, AgeCategory), List[(Region, predicted_vaccinations]>
         */
        JavaPairRDD<Tuple2<LocalDate, AgeCategory>, MyIterable> dateAndAgePair = predictedVaccinations.mapToPair(x ->
                new Tuple2<>(
                        new Tuple2<>(x._2._1(), x._1._3()),
                        new MyIterable(new Tuple2<>(x._1._2(), x._2._2()))
                )).reduceByKey(MyIterable::addAll) //pattern to perform a groupByKey
                .mapValues(x -> {
                    Tuple2Comparator comparator = new Tuple2Comparator();
                    x.descendingSort(comparator); // sorting
                    x.sublist(0, 5); // take range [0:4]
                    return x;
                });

        List<Tuple2<Tuple2<LocalDate, AgeCategory>, MyIterable>> results = dateAndAgePair.collect();
        for (Object o : results) {
            System.out.println(o);
        }

        try {
            TimeUnit.MINUTES.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        sc.stop();
    }


    /**
     * Custom MapFunction to perform Linear Regression by using: "org.apache.commons.math3.stat.regression.SimpleRegression"
     * The Regression takes points as (x, y) coordinates; in particular x= date_as_double, y=female_vaccinations_as_double.
     * These points should be representative of the same month, region and ageCategory.
     * In the end the Regression makes a prediction on the first day of the next month.
     */
    private static class Predictor implements Function {
        @Override
        public Tuple2<LocalDate, Double> call(Object o) {
            MyIterable myIterable = (MyIterable) o;
            //here do regression
            SimpleRegression R = new SimpleRegression();

            for (Object item : myIterable.getList()) {
                Tuple2<LocalDate, Integer> t = (Tuple2<LocalDate, Integer>) item;
                Date date = Date.from(t._1().atStartOfDay(ZoneId.systemDefault()).toInstant());
                R.addData(date.getTime(), (double) t._2()); //add points to the Regression Object
            }

            Tuple2<LocalDate, Integer> elem = (Tuple2<LocalDate, Integer>) myIterable.getList().get(0);
            // from 2021-02-13 -> 2021-03-01
            LocalDate d = elem._1().withDayOfMonth(1).plusMonths(1);
            Date dateToPredict = Date.from(d.atStartOfDay(ZoneId.systemDefault()).toInstant());
            double xToPredict = (double) dateToPredict.getTime();

            return new Tuple2<>(d, round(abs(R.predict(xToPredict)), 3));

        }
    }

}
