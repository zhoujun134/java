package study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 通用的load和save操作
 * @author Administrator
 *
 */
public class GenericLoadSave {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("GenericLoadSave");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		Dataset<Row> usersDF = sqlContext.read().load(
				"hdfs://spark1:9000/users.parquet");
		usersDF.select("name", "favorite_color").write()
				.save("hdfs://spark1:9000/namesAndFavColors.parquet");   
	}
	
}
