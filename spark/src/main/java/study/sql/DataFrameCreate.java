package study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 使用json文件创建DataFrame
 * @author Administrator
 *
 */
public class DataFrameCreate {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("DataFrameCreate");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		Dataset<Row> df = sqlContext.read().json("hdfs://spark1:9000/students.json");
		
		df.show();  
	}
	
}
