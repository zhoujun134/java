package study.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static java.util.Arrays.*;

/**
 * 实时wordcount程序
 * @author Administrator
 *
 */
public class WordCount {
	
	public static void main(String[] args) throws Exception {
		String[] strs = {"/home/zhoujun/IdeaProjects/java/out/artifacts/spark_jar/spark.jar"};
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setJars(strs)
				.setAppName("WordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

		jssc.checkpoint("/home/zhoujun/IdeaProjects/java/out/checkpoint/");
		jssc.sparkContext().setLogLevel("WARN");
		JavaDStream<String> lines = jssc.socketTextStream("localhost", 9999);

//		lines = lines.window(Durations.seconds(6), Durations.seconds(2));

		JavaDStream<String> words = lines.flatMap(s -> {
			System.out.println("收到的数据为： " + s + "  当前时间： " + new Date());
			return asList(s.split(" ")).iterator();
		});

		JavaPairDStream<String, Integer> pairs = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, Integer> call(String word)
							throws Exception {
						return new Tuple2<String, Integer>(word, 1);
					}
					
				});

//		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
//				new Function2<Integer, Integer, Integer>() {
//					private static final long serialVersionUID = 1L;
//					@Override
//					public Integer call(Integer v1, Integer v2) throws Exception {
//						return v1 + v2;
//					}
//				});

//		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKeyAndWindow(
//				(x, y) -> x + y,
//				(x, y) -> x - y,
//				Durations.seconds(6),
//				Durations.seconds(4));
		JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
			@Override
			public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
				Integer updateValue = 0;
				if(state.isPresent()) updateValue = state.get();
				for(Integer value: values) updateValue += value;

				return Optional.of(updateValue);
			}
		});

		Thread.sleep(5000);  
		wordCounts.print();

		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
	
}
