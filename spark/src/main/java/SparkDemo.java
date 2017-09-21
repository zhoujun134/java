import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
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
//        SparkConf conf = new SparkConf().setMaster("local").setAppName("demo");
//        JavaSparkContext sc = new JavaSparkContext(conf);
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

        new JavaSparkContext(new SparkConf().setMaster("local").setAppName("demo"))
                .parallelize(new ArrayList<String>(){{
            this.add("hello java a world");
            this.add("hello scala a world");
            this.add("haha  heihei");
        }}).flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(t -> new Tuple2<String,Integer>(t,1))
                .reduceByKey((v1, v2) -> v1+v2)
                .foreach(System.out :: println);

//                .foreach(s -> System.out.println(s));

    }
}
