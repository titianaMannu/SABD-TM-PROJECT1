import org.apache.log4j.lf5.LogLevel;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import query.Query1;
import query.Query2;
import utils.enums.Constants;

import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        boolean useDebugMode;
        useDebugMode = args.length > 0 && args[0].equals("-D");
        SparkConf conf = new SparkConf()
                .setMaster(Constants.SPARK_MASTER.getString())
                .setAppName(Constants.PROJECT_NAME.getString());
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel(LogLevel.WARN.toString());
        Logger log = sc.sc().log();

        Query1 q1 = new Query1(useDebugMode);
        q1.executeQuery(sc);
        log.warn("execution time for query 1: " + q1.getLastExecutionTime() + " ms");
        log.warn("see output in: " + Constants.OUTPUT_PATH_Q1.getString());

        Query2 q2 = new Query2(useDebugMode);
        q2.executeQuery(sc);
        log.warn("execution time for query 2: " + q2.getExecutionTime() + " ms");
        log.warn("see output in: " + Constants.OUTPUT_PATH_Q2.getString());

        try {
            TimeUnit.MINUTES.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        sc.stop();
    }
}

