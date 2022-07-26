package cam.bigdatacompany.spark;

import cam.bigdatacompany.spark.model.Person;
import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class MapTrans {
    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local", "Map Transformation Spark");

        JavaRDD<String> rawData = javaSparkContext.textFile("C:\\Users\\Msı\\Desktop\\person.csv");


        //JavaRDD<Person> loadPerson = rawData.map(new Function<String, Person>() {
        //    @Override
        //    public Person call(String line) throws Exception {
        //        String[] data = line.split(",");
        //        Person p = new Person();
        //        p.setFirst_name(data[0]);
        //        p.setLast_name(data[1]);
        //        p.setEmail(data[2]);
        //        p.setGender(data[3]);
        //        p.setCountry(data[4]);
        //        return p;
        //    }
        //});

        // eski yontem
        //loadPerson.foreach(new VoidFunction<Person>() {
        //    @Override
        //    public void call(Person person) throws Exception {
        //        System.out.println("Adi: " + person.getFirst_name() + " Soyadi: " + person.getLast_name());
        //    }
        //});


        //JavaRDD<Person> personFromTM = loadPerson.filter(new Function<Person, Boolean>() {
        //    @Override
        //    public Boolean call(Person person) throws Exception {
        //        if (person.getCountry().equals("UK") && person.getGender().equals("male"))
        //            return true;
        //        return false;
        //    }
        //});
        //System.out.println("person count: " + personFromTM.count());
        //personFromTM.foreach(new VoidFunction<Person>() {
        //    @Override
        //    public void call(Person person) throws Exception {
        //        System.out.println(person.getFirst_name() + " " + person.getLast_name() + " " + person.getCountry());
        //    }
        //});

        //JavaRDD<String> stringJavaRDD = rawData.flatMap(new FlatMapFunction<String, String>() {
        //    @Override
        //    public Iterator<String> call(String s) throws Exception {
        //        return Arrays.asList(s.split(",")).iterator();
        //    }
        //});
        //System.out.println(stringJavaRDD.count());

        //System.out.println(rawData.count());
        //JavaRDD<String> distData = rawData.distinct();
        //System.out.println(distData.count());

        JavaRDD<Person> loadPerson = rawData.map(new Function<String, Person>() {
            @Override
            public Person call(String line) throws Exception {
                String[] data = line.split(",");
                Person p = new Person();
                p.setFirst_name(data[0]);
                p.setLast_name(data[1]);
                p.setEmail(data[2]);
                p.setGender(data[3]);
                p.setCountry(data[4]);
                return p;
            }
        });
        //JavaPairRDD<String, String> pairRDD = loadPerson.mapToPair(new PairFunction<Person, String, String>() {
        //    @Override
        //    public Tuple2<String, String> call(Person person) throws Exception {
        //        return new Tuple2<>(person.getEmail(), person.getCountry());
        //    }
        //});
        //pairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
        //    @Override
        //    public void call(Tuple2<String, String> data) throws Exception {
        //        System.out.println("Key: " + data._1 + "Value: " + data._2);
        //    }
        //});

        JavaPairRDD<String, Person> pairRDD = loadPerson.mapToPair(new PairFunction<Person, String, Person>() {
            @Override
            public Tuple2<String, Person> call(Person person) throws Exception {
                return new Tuple2<>(person.getEmail(), person);
            }
        });

        JavaPairRDD<String, Iterable<Person>> groupData = pairRDD.groupByKey();

        groupData.foreach(new VoidFunction<Tuple2<String, Iterable<Person>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Person>> data) throws Exception {
                System.out.println("Key: " + data._1 + " Count: " + Iterables.size(data._2));
            }
        });

    }
}
