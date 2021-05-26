import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import query.Query1;
import query.Query2;
import utils.enums.Constants;

import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster(Constants.MASTER_URL.getString())
                .setAppName("VaccinationQueries");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        Query1 q1 = new Query1(false);
        q1.executeQuery(sc);
        System.out.println("execution time for query 2: " + q1.getLastExecutionTime());
        System.out.println("see output in: " + Constants.OUTPUT_PATH_Q1.getString());

        Query2 q2 = new Query2(false);
        q2.executeQuery(sc);
        System.out.println("execution time for query 2: " + q2.getExecutionTime());
        System.out.println("see output in: " + Constants.OUTPUT_PATH_Q2.getString());

        try {
            TimeUnit.MINUTES.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        sc.stop();
    }
}

