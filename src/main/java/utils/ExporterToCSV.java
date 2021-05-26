package utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class ExporterToCSV {
    private final String schema;
    private final String outputFolder;

    public ExporterToCSV(String schema, String outputFolder) {
        this.schema = schema;
        this.outputFolder = outputFolder;
    }

    public void generateCSV(JavaSparkContext sc, JavaRDD<Row> rowRDD){
        SQLContext sqlContext = new SQLContext(sc);
        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : this.schema.split(",")) {
            fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(fields);
        // Apply the schema to the RDD.
        Dataset<Row> peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);
        peopleDataFrame.coalesce(1).write()
                .option("header", "true")
                .option("sep", ",")
                .mode("overwrite")
                .csv(this.outputFolder);
    }
}
