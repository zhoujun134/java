package com.classstudy;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple1;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class ReduceByKeyTest {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("demo").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.parallelize(new ArrayList<String>(){{
            this.add("http://www.baidu.com/about.html");
            this.add("http://www.ali.com/index.html");
            this.add("http://www.sina.com/first.html");
            this.add("http://www.sohu.com/index.html");
            this.add("http://www.baidu.com/index.jsp");
            this.add("http://www.sina.com/help.html");
        }});
        HashMap<String, Integer> dateResult = new HashMap<>();
        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> pairRDD2 = pairRDD.reduceByKey(new Partitioner() {
            @Override
            public int getPartition(Object key) {
                String keystr = (String)key;
                String[] arr = keystr.split("/");
                boolean[] isr = {false};
                dateResult.keySet().forEach(s -> {
                    if(arr[2].equals(s)) isr[0] = true;
                });

                if(isr[0]) {
                    dateResult.put(arr[2], dateResult.get(arr[2])+1);
                    System.out.println("true: map的长度： " + dateResult.size());
                    System.out.println("date: " + arr[2] + "   "+ dateResult.get(arr[2]));
                    return 0;
                }
                else {
                    dateResult.put(arr[2], 1);
                    System.out.println("false:  map的长度： " + dateResult.size());
                    System.out.println("date: " + arr[2] + "   "+ dateResult.get(arr[2]));
                    return 1;
                }
            }

            @Override
            public int numPartitions() {
                return 2;
            }
        }, (i1, i2) -> i1 + i2);



        System.out.println("****** :   " + pairRDD2.getNumPartitions());
        System.out.println("****** :   " + pairRDD2.glom().collect());
    }
}
