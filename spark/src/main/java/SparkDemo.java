import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by zhoujun on 2017/9/18.
 */
public class SparkDemo {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("demo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println(sc);
    }
}
