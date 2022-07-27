package cam.bigdatacompany.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkSqlFirst {
    public static void main(String[] args) {

        StructType schema = new StructType().add("firstName", DataTypes.StringType)
                .add("lastName", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("gender", DataTypes.StringType)
                .add("country", DataTypes.StringType)
                .add("age", DataTypes.IntegerType);

        SparkSession sparkSession = SparkSession.builder().master("local").appName("First Exam").getOrCreate();
        //Dataset<Row> rowDataset = sparkSession.read().csv("C:\\Users\\Msı\\Desktop\\person.csv");
        //Dataset<Row> rowDataset = sparkSession.read().option("header", true).csv("C:\\Users\\Msı\\Desktop\\person.csv");
        Dataset<Row> rowDataset = sparkSession.read().option("header", true).schema(schema).csv("C:\\Users\\Msı\\Desktop\\person.csv");

        //Dataset<Row> withColumn = rowDataset.withColumn("firstNameTest", rowDataset.col("firstName"));
        //withColumn.show();

        Dataset<Row> selectDS = rowDataset.select("firstName", "email", "country", "age");
        //selectDS.show();

        //Dataset<Row> ukSet = selectDS.filter(selectDS.col("country").equalTo("uk"));
        //Dataset<Row> ukSet = selectDS.filter(selectDS.col("country").equalTo("uk")
        //        .and(selectDS.col("age").gt(20)).and(selectDS.col("email").contains("google")));
        //ukSet.show();

        //Dataset<Row> filter = selectDS.filter(selectDS.col("country").equalTo("uk").or(selectDS.col("country").equalTo("turkey")));
        //Dataset<Row> filter = selectDS.filter("country = 'fr' or country = 'tr' ");
        //filter.show();

        //Dataset<Row> ds50 = selectDS.filter("age > 50").sort("age");
        //Dataset<Row> ds50 = selectDS.filter("age > 50").orderBy("age");
        //ds50.show();

        //rowDataset.show();
        //rowDataset.printSchema();

        //Dataset<Row> rowDataset1 = rowDataset.select("_c0", "_c2");
        //rowDataset1.show();

        //Dataset<Row> country = selectDS.groupBy("country").count();
        Dataset<Row> country = selectDS.groupBy("country").max();
        //Dataset<Row> country = selectDS.groupBy("firstName","country").count();
        country.show();
    }
}
