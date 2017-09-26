package com.classstudy;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.math.Ordering;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by zhoujun on 2017/9/18.
 */
public class SparkDemo {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("demo");
        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        sc.parallelize(new ArrayList<String>(){{
//            this.add("hello java a world");
//            this.add("hello scala a world");
//            this.add("haha  heihei");
//        }}).flatMap(new FlatMapFunction<String, String>() {
//            public Iterator<String> call(String s) throws Exception {
//                return Arrays.asList(s.split(" ")).iterator();
//            }
//        }).mapToPair(new PairFunction<String, String, Integer>() {
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                return new Tuple2<String, Integer>(s,1);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer+integer2;
//            }
//        }).foreach(new VoidFunction<Tuple2<String, Integer>>(){
//            public void call(Tuple2<String, Integer> s) throws Exception {
//                System.out.println(s);
//            }
//        });

//        new JavaSparkContext(new SparkConf().setMaster("local").setAppName("demo"))
//                .parallelize(new ArrayList<String>(){{
//            this.add("hello java a world");
//            this.add("hello scala a world");
//            this.add("haha  heihei");
//        }}).flatMap(s -> Arrays.asList(s.split(" ")).iterator())
//                .mapToPair(t -> new Tuple2<String,Integer>(t,1))
//                .reduceByKey((v1, v2) -> v1+v2)
////                .foreach(System.out :: println);
//                .foreach(s -> System.out.println(s));

//        System.out.println("用第一个单词作为key........");
//        sc.parallelize(new ArrayList<String>(){{
//            this.add("hello java a world");
//            this.add("No scala a world");
//            this.add("haha  heihei");
//        }})
//                .mapToPair(t -> new Tuple2<String,String>(t.split(" ")[0], t))
//                .foreach(s -> System.out.println(s));


//        System.out.println("长度大于3的行.......");
//        sc.parallelize(new ArrayList<String>(){{
//            this.add("hello java a world");
//            this.add("No scala a world");
//            this.add("Good  heihei");
//        }})
////                .filter(s -> s.split(" ").length > 3)
//                .foreach(s -> System.out.println(s));

//        System.out.println("方式二。。。。。");
//        JavaRDD<String> rdd = sc.parallelize(new ArrayList<String>(){{
//            this.add("hello java a world");
//            this.add("No scala a world");
//            this.add("haha  heihei");
//        }});
//        JavaPairRDD<String, String> pairRDD = rdd.mapToPair(new PairFunction<String, String, String>() {
//            @Override
//            public Tuple2<String, String> call(String s) throws Exception {
//                return new Tuple2<String, String>(s.split("")[0], s);
//            }
//        });
//        pairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
//            @Override
//            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
//                System.out.println(stringStringTuple2);
//            }
//        });
//        JavaRDD<String> rdd = sc.parallelize(new ArrayList<String>(){{
//            this.add("hello java a world");
//            this.add("No scala a world");
//            this.add("haha  heihei");
//        }});
//        rdd.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String v1) throws Exception {
//                return v1.split(" ").length > 3;
//            }
//        }).foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });
//
        JavaRDD<Integer> rddlist = sc.parallelize(Arrays.asList(1,2,32,232,4,2,2323,52,3,66));
        JavaPairRDD<Integer, Integer> pairRDD = rddlist.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer, 1);
            }
        });
        pairRDD.reduceByKey(new Partitioner() {
            @Override
            public int getPartition(Object key) {
                System.out.println("获取分区数量：" + key);
                return 0;
            }

            @Override
            public int numPartitions() {
                return 2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                System.out.println(integerIntegerTuple2);
            }
        });

    }
}
