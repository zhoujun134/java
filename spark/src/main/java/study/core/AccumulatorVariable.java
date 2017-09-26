package study.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 累加变量
 * @author Administrator
 *
 */
public class AccumulatorVariable {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("Accumulator") 
				.setMaster("spark://sparkMaster:7077");
		JavaSparkContext sc = new JavaSparkContext(conf);
	
		// 创建Accumulator变量
		// 需要调用SparkContext的 accumulator()方法
//		final Accumulator<Integer> sum = sc.accumulator(0);
		final int[] sum={0};
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);


		numbers.foreach(integer -> sum[0] += sum[0]);
		
		// 在driver程序中，可以调用Accumulator的value()方法，获取其值
		System.out.println(sum[0]);
		
		sc.close();
	}
	
}
