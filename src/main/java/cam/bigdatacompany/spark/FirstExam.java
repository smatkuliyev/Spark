package cam.bigdatacompany.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class FirstExam {
    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local", "First Exam Spark");

        /*
        JavaRDD<String> firstData = javaSparkContext.textFile("C:\\Users\\Msı\\Desktop\\firstdata.txt");
        System.out.println(firstData.count());
        System.out.println(firstData.first());
         */

        List<String> data = Arrays.asList("big data", "elasticSearch", "first", "spark", "hadoop");
        JavaRDD<String> secondData = javaSparkContext.parallelize(data);
        System.out.println(secondData.count());
        System.out.println(secondData.first());
    }
}
