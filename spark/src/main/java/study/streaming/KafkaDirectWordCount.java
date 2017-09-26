package study.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * 基于Kafka Direct方式的实时wordcount程序
 * @author Administrator
 *
 */
public class KafkaDirectWordCount {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("KafkaDirectWordCount");  
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		// 首先，要创建一份kafka参数map
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", 
				"192.168.1.107:9092,192.168.1.108:9092,192.168.1.109:9092");
		
		// 然后，要创建一个set，里面放入，你要读取的topic
		// 这个，就是我们所说的，它自己给你做的很好，可以并行读取多个topic
		Set<String> topics = new HashSet<String>();
		topics.add("WordCount");
		
		// 创建输入DStream
		JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
				jssc, 
				String.class, 
				String.class, 
				StringDecoder.class, 
				StringDecoder.class, 
				kafkaParams, 
				topics);

		// 执行wordcount操作
		JavaDStream<String> words = lines.flatMap(
				
				new FlatMapFunction<Tuple2<String,String>, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<String> call(Tuple2<String, String> tuple)
							throws Exception {
						return Arrays.asList(tuple._2.split(" ")).iterator();
					}
					
				});
		
		JavaPairDStream<String, Integer> pairs = words.mapToPair(
				
				new PairFunction<String, String, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String word) throws Exception {
						return new Tuple2<String, Integer>(word, 1);
					}
					
				});
		
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
				
				new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
					
				});
		
		wordCounts.print();
		
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		jssc.close();
	}
	
}
