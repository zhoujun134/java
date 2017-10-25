package com.classstudy;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by zhoujun on 17-10-25.
 */
public class UnionTest {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SPARKDEMO");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.parallelize(new ArrayList<String>(){{
            this.add("Hello");
            this.add("Hi");
            this.add("compare");
        }},2);
        JavaPairRDD<String, String> javaPairRDD1 = rdd1.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s,"1");
            }
        });

        JavaPairRDD<String, String> pairRDD1 = javaPairRDD1.partitionBy(new Partitioner() {
            @Override
            public int getPartition(Object key) {
                return 3;
            }

            @Override
            public int numPartitions() {
                return 3;
            }
        });



        JavaRDD<String> rdd2 = sc.parallelize(new ArrayList<String>(){{
            this.add("Hello");
            this.add("Hi");
            this.add("due");
        }},3);

        JavaPairRDD<String, String> javaPairRDD2 = rdd2.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s,"1");
            }
        });


        JavaPairRDD<String, String> pairRDD2 = javaPairRDD2.partitionBy(new Partitioner() {
            @Override
            public int getPartition(Object key) {
                return 3;
            }

            @Override
            public int numPartitions() {
                return 3;
            }
        });



        JavaPairRDD<String, String> pairRDD3 = pairRDD1.union(pairRDD2);
//        pairRDD3.foreach(s -> System.out.println(s));

        System.out.println("rdd1 of partitionNum: " + pairRDD1.getNumPartitions());
        System.out.println("rdd2 of partitionNum: " + pairRDD2.glom().collect());
        System.out.println("rdd3 of partitionNum: " + pairRDD3.glom().collect());






    }
}
