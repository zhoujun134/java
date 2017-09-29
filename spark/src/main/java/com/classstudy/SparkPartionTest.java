package com.classstudy;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class SparkPartionTest {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SPARKDEMO");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.parallelize(
                Arrays.asList("KK6JKQ", "Ve3UoW", "kk6jlk", "W6BB"));
        JavaRDD<String> result = rdd.mapPartitions(
                new FlatMapFunction<Iterator<String>, String>() {
                    public Iterator<String> call(Iterator<String> input) {
                        ArrayList<String> content = new ArrayList<String>();
                        System.out.println("数据： " + input);
                        while (input.hasNext()){
                            content.add(input.next());
                        }
                        return content.iterator();
                    }});
        System.out.println(StringUtils.join(result.collect(), ","));
    }
}
