package study.core.upgrade;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCount {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("WordCount");     
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("hdfs://192.168.0.103:9000/test/hello.txt");
		
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			
			private static final long serialVersionUID = 1L;
			
			@Override
			public Iterator<String> call(String line) throws Exception {
				System.out.println("==================" + line + "==================");  
				Thread.sleep(10 * 1000);  
				return Arrays.asList(line.split(" ")).iterator();
			}
			
		});
		
		JavaPairRDD<String, Integer> pairs = words.mapToPair(
				
				new PairFunction<String, String, Integer>() {
					
					private static final long serialVersionUID = 1L;
					
					@Override
					public Tuple2<String, Integer> call(String word) throws Exception {
						System.out.println("==================" + word + "==================");  
						Thread.sleep(10 * 1000);  
						return new Tuple2<String, Integer>(word, 1);
					}
					
				});
		
		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(
				
				new Function2<Integer, Integer, Integer>() {
					
					private static final long serialVersionUID = 1L;
		
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						Thread.sleep(30 * 1000);  
						return v1 + v2;
					}
					
				});
		
		List<Tuple2<String, Integer>> wordCountList = wordCounts.collect();
		for(Tuple2<String, Integer> wordCount : wordCountList) {
			System.out.println(wordCount);  
		}
		
		sc.close();
	}
	
}
