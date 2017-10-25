package study.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.AccumulatorV2;
import study.core.tmpclass.MyAccumulatorV2;

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
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 创建Accumulator变量
		// 需要调用SparkContext的 accumulator()方法
//		final AccumulatorV2<Integer> sum = sc.accumulator(0);
//		final int[] sum={0};
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);

//		numbers.foreach(integer -> sum[0] = sum[0] + integer);
//		numbers.foreach(s -> System.out.println(s));
		
		// 在driver程序中，可以调用Accumulator的value()方法，获取其值
//		System.out.println(sum.value());
		MyAccumulatorV2 myAccumulatorV2 = new MyAccumulatorV2();
		sc.close();
	}

}