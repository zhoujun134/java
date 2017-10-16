package com.classstudy;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple1;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by zhoujun on 17-10-11.
 */
public class Combinkey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("demo");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(new ArrayList<Tuple2<String, Integer>>(){{
            this.add(new Tuple2<>("x1",2));
            this.add(new Tuple2<>("x2",9));
            this.add(new Tuple2<>("x1",7));
            this.add(new Tuple2<>("x3",5));
            this.add(new Tuple2<>("x4",3));

        }});

//
//        JavaPairRDD<String, Tuple2<Integer, Integer>> results = rdd.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Tuple2<Integer, Integer>>() {
//            @Override
//            public Tuple2<String, Tuple2<Integer, Integer>> call(Tuple2<String, Integer> t) throws Exception {
//                return new Tuple2<>(t._1(), new Tuple2<>(t._2(), 1));
//            }
//        }).reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
//            @Override
//            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) throws Exception {
//                return new Tuple2<Integer, Integer>(t1._1()+ t2._1(), t1._2()+t2._2());
//            }
//        });
//
//        results.foreach(s -> System.out.println(s));
//

        JavaPairRDD<String, Integer> rdd1 = rdd.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2;
            }
        });

//        JavaPairRDD<String, Tuple2<Integer, Integer>> results = rdd1.combineByKey(new Function<Integer, Tuple2<Integer, Integer>>() {
//            @Override
//            public Tuple2<Integer, Integer> call(Integer v) throws Exception {
//                return new Tuple2<>(v, 1);
//            }
//        }, new Function2<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>() {
//            @Override
//            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> vltup, Integer vkey) throws Exception {
//                return new Tuple2<Integer, Integer>(vltup._1() + vkey, vltup._2() + 1);
//            }
//        }, new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
//            @Override
//            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) throws Exception {
//                return new Tuple2<Integer, Integer>(t1._1() + t2._1(),t1._2() + t2._2());
//            }
//        });
//
//        results.foreach(s -> System.out.println(s));

        JavaPairRDD<String, Tuple2<Integer, Integer>> results = rdd1.combineByKey(
                v -> new Tuple2<Integer, Integer>(v ,1),
                (vTup, vTupKey) -> new Tuple2<Integer, Integer>(vTup._1() + vTupKey, vTup._2() + 1),
                (t1, t2) -> new Tuple2<Integer, Integer>(t1._1() + t2._1(),t1._2() + t2._2())
        );
        results.foreach(stringTuple2Tuple2 -> System.out.println(stringTuple2Tuple2));

        JavaPairRDD<String, Tuple1<Integer>[]> results2 = rdd1.combineByKey(new Function<Integer, Tuple1<Integer>[]>() {
            @Override
            public Tuple1<Integer>[] call(Integer integer) throws Exception {
                Tuple1[] tuple1s = new Tuple1[1];
                tuple1s[0] = new Tuple1<Integer>(integer);
                return tuple1s;
            }
        }, new Function2<Tuple1<Integer>[], Integer, Tuple1<Integer>[]>() {
            @Override
            public Tuple1<Integer>[] call(Tuple1<Integer>[] tuple1s, Integer integer) throws Exception {
                Tuple1[] tuple1s1 = new Tuple1[tuple1s.length + 1];
                for(int i=0; i< tuple1s.length; i++) {
                    tuple1s1[i] = tuple1s[i];
                }
                tuple1s1[tuple1s.length] = new Tuple1<Integer>(integer);
                return tuple1s1;
            }
        }, new Function2<Tuple1<Integer>[], Tuple1<Integer>[], Tuple1<Integer>[]>() {
            @Override
            public Tuple1<Integer>[] call(Tuple1<Integer>[] tuple1s, Tuple1<Integer>[] tuple1s2) throws Exception {
                Tuple1[] result = new Tuple1[tuple1s.length+tuple1s2.length];
                int flag = 0;
                for(int i=0; i< tuple1s.length; i++){
                    result[i] = tuple1s[i];
                    flag = i;
                }
                for(int i=0; i<tuple1s2.length; i++){
                    flag++;
                    result[flag] = tuple1s2[i];
                }
                return result;
            }
        });

           results2.foreach(stringTuple2 -> {
             System.out.println(stringTuple2._1()+"   val: "+ stringTuple2._2().length);
           });
    }
}
