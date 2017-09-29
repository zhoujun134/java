package com.classstudy;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class JoinUnionDemo {
    public static void main(String[] xx){
    	SparkConf conf = new SparkConf();
    	conf.setMaster("local");
    	conf.setAppName("WordCounter");

    	JavaSparkContext ctx = new JavaSparkContext(conf);
    	//����RDD��1��ͨ����ȡ�ⲿ�洢 ----- ��Ⱥ����ʹ�� 2��ͨ���ڴ��еļ���

    	List<String> lines1 = new ArrayList<String>();
    	lines1.add("http://www.baidu.com/about.html");
    	lines1.add("http://www.ali.com/index.html");
    	lines1.add("http://www.sina.com/first.html");
    	lines1.add("http://www.sohu.com/index.html");
    	lines1.add("http://www.baidu.com/index.jsp");
    	lines1.add("http://www.sina.com/help.html");
    	JavaRDD<String> rdd1 = ctx.parallelize(lines1, 2);

    	List<String> lines2 = new ArrayList<String>();
    	lines2.add("Hello");
    	lines2.add("How");
    	lines2.add("Good");    	
    	JavaRDD<String> rdd2 = ctx.parallelize(lines2);
    

    	//JavaRDD<String> rdd3 = rdd1.union(rdd2);
    	//JavaRDD<String> rdd3 = rdd1.intersection(rdd2);
    	//System.out.println(rdd3.collect());
    	//rdd3.foreach(System.out::println);
    	
        JavaPairRDD<String, Integer> mapRdd1 = rdd1.mapToPair(new PairFunction<String, String, Integer>() {  
    	      @Override
    	      public Tuple2<String, Integer> call(String s) throws Exception {  
    	          return new Tuple2<String, Integer>(s, 2);  
    	      }
        });
      	System.out.println(mapRdd1.getNumPartitions());
        
      	JavaPairRDD<String, Integer> mapRdd1_new = mapRdd1.partitionBy(new Partitioner() {
			@Override
			public int numPartitions() {
				return 5;
			}
			
			@Override
			public int getPartition(Object arg0) {
				URL url = null;
				try {
					url = new URL((String)arg0);
				} catch (MalformedURLException e) {
					e.printStackTrace();
				}
				String hostName = url.getHost();
				int hash = hostName.hashCode();
				System.out.println("hostName:" + hostName + " hash:" + hash);
				int index = hash % numPartitions();
				if(index < 0){
					index = 0;
				}
				return index;
			}
		});
        System.out.println(mapRdd1_new.getNumPartitions());
        System.out.println(mapRdd1_new.glom().collect());
      	
//      	
//        JavaPairRDD<String, Integer> mapRdd2 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {  
//  	      @Override
//  	      public Tuple2<String, Integer> call(String s) throws Exception {  
//  	          return new Tuple2<String, Integer>(s, 1);  
//  	      }
//        });     	
//        JavaPairRDD<String, Tuple2<Integer, Integer>>mapRdd3 = mapRdd1.join(mapRdd2);
//        System.out.println(mapRdd3.collect());
         
    }
}
