package cam.bigdatacompany.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class ActionApp {
    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local", "Map Transformation Spark");
        JavaRDD<String> rawData = javaSparkContext.textFile("C:\\Users\\Msı\\Desktop\\person.csv");

        System.out.println(rawData.count());
        System.out.println(rawData.first());
        System.out.println(rawData.take(2));
        //System.out.println(rawData.saveAsTextFile("C:\\Users\\Msı\\Desktop\\person.csv"));
        rawData.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
