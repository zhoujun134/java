package com.classstudy.classtest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple1;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created by zhoujun on 17-10-18.
 */
public class SortByKeyTest {
    public static void main(String[] args){
        List<Tuple2<Person, Integer>> data = new ArrayList<>();

        data.add(new Tuple2<>(new Person("sxaxsa",23), 1000));
        data.add(new Tuple2<>(new Person("sxaxsa",12), 1000));
        data.add(new Tuple2<>(new Person("qww",79), 1000));
        data.add(new Tuple2<>(new Person("dff",78), 1000));
        data.add(new Tuple2<>(new Person("sd",24), 43434));
        data.add(new Tuple2<>(new Person("jkjk",23), 1000));
        data.add(new Tuple2<>(new Person("xcc",23), 1000));
        data.add(new Tuple2<>(new Person("df",12), 3434));
        data.add(new Tuple2<>(new Person("gh",16), 14300));
        data.add(new Tuple2<>(new Person("bv",56), 32323));
        data.add(new Tuple2<>(new Person("bv",43), 4323));
        data.add(new Tuple2<>(new Person("jk",34), 4545));
        data.add(new Tuple2<>(new Person("ol",34), 45454));

        SparkConf conf = new SparkConf().setMaster("local").setAppName("demo");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Tuple2<Person, Integer>> rdd = sc.parallelize(data);

        JavaPairRDD<Person, Integer> pairRDD = rdd.mapToPair(new PairFunction<Tuple2<Person, Integer>, Person, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<Person, Integer> call(Tuple2<Person, Integer> personIntegerTuple2) throws Exception {
                return personIntegerTuple2;
            }
        });

        JavaPairRDD<Person, Integer> pairRDD2 = pairRDD.sortByKey(new MyComparable());

        pairRDD2.foreach(personIntegerTuple2 -> {
            System.out.print(" person: " + personIntegerTuple2._1());
            System.out.println(" salary: " + personIntegerTuple2._2());
        });



    }
}
class MyComparable implements Serializable, Comparator<Person>{
            @Override
            public int compare(Person person, Person t1) {
                if(person.getName().equals(t1.getName())){
                    if(person.getAge() < person.getAge()) return 1;
                    else return -1;
                }
                return person.getName().compareTo(t1.getName());
            }
}