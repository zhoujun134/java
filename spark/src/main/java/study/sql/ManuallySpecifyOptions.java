package study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 手动指定数据源类型
 * @author Administrator
 *
 */
public class ManuallySpecifyOptions {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("ManuallySpecifyOptions");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		Dataset<Row> peopleDF = sqlContext.read().format("json")
				.load("hdfs://spark1:9000/people.json");
		peopleDF.select("name").write().format("parquet")
				.save("hdfs://spark1:9000/peopleName_java");     
	}
	
}
