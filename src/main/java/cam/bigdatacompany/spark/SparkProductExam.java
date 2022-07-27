package cam.bigdatacompany.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkProductExam {
    public static void main(String[] args) throws AnalysisException {

        StructType schema = new StructType()
                .add("first_name", DataTypes.StringType)
                .add("last_name", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("gender", DataTypes.StringType)
                .add("country", DataTypes.StringType)
                .add("price", DataTypes.DoubleType)
                .add("product", DataTypes.StringType);

        SparkSession sparkSession = SparkSession.builder().master("local").appName("First Exam").getOrCreate();
        //Dataset<Row> jsonRawData = sparkSession.read().schema(schema).json("C:\\Users\\Msı\\Downloads\\MOCK_DATA.json");
        Dataset<Row> jsonRawData = sparkSession.read().schema(schema).option("multiline", true).json("C:\\Users\\Msı\\Downloads\\MOCK_DATA.json");
        //jsonRawData.show();
        //Dataset<Row> sum = jsonRawData.groupBy("country").sum("price");
        //Dataset<Row> sum = jsonRawData.groupBy("country", "product").sum("price");
        //sum.show();

        //Dataset<Row> sum = jsonRawData.groupBy("country", "product").count();
        //sum.sort(functions.desc("count")).show();

        //Dataset<Row> sum = jsonRawData.groupBy("country").avg("price");
        //sum.show();

        //session bazli
        jsonRawData.createOrReplaceTempView("product");
        //Dataset<Row> sqlDS = sparkSession.sql("select * from product");
        Dataset<Row> sqlDS = sparkSession.sql("select first_name,email,country from product where country = 'Ukraine' or country = 'Brazil' ");
        sqlDS.show();

        //clustes bazli
        //jsonRawData.createGlobalTempView("product");
    }
}
