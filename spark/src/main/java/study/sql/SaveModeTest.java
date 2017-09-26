package study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * SaveModel示例
 * @author Administrator
 *
 */
public class SaveModeTest {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("SaveModeTest");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		Dataset<Row> peopleDF = sqlContext.read().format("json")
				.load("hdfs://spark1:9000/people.json"); 
//		peopleDF.explode("hdfs://spark1:9000/people_savemode_test", "json", SaveMode.Append);
	}
	
}
