package com.classstudy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class PartitionDemo {
    public static void main(String[] xx){
    	SparkConf conf = new SparkConf();
    	conf.setMaster("local");
    	conf.setAppName("WordCounter");
    	JavaSparkContext ctx = new JavaSparkContext(conf);

    	List<String> lines = new ArrayList<String>();
    	lines.add("Hello How are you");
    	lines.add("No Fine thanks a lot ddddddddddd");
    	lines.add("Good are ok");
    	lines.add("Good is ok");
    	JavaRDD<String> rdd1 = ctx.parallelize(lines, 2);
        System.out.println("rdd1 partitioner:" + rdd1.partitioner());
        System.out.println("rdd1: " + rdd1.getNumPartitions());
        System.out.println("rdd1 :" + rdd1.glom().collect());
        
        JavaRDD<String> words = rdd1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        System.out.println("words partitioner:" + words.partitioner());
        System.out.println("words: " + words.getNumPartitions());
        System.out.println("words :" + words.glom().collect());

      JavaPairRDD<String, Integer> mapRdd = words.mapToPair(new PairFunction<String, String, Integer>() {  
	      @Override
	      public Tuple2<String, Integer> call(String s) throws Exception {  
	          return new Tuple2<String, Integer>(s, 1);  
	      }
      });

//      System.out.println("mapRdd partitioner:" + mapRdd.partitioner());
//      System.out.println("mapRdd :" + mapRdd.getNumPartitions());
//      System.out.println("mapRdd :" + mapRdd.glom().collect());   

      JavaPairRDD<String, Integer> mapRdd2 = mapRdd.partitionBy(new Partitioner() {
			@Override
			public int numPartitions() {
				return 4;
			}

			@Override
			public int getPartition(Object arg0) {
				int hashCode = arg0.hashCode();
				int index = hashCode % numPartitions();
				//System.out.println("hashCode = " + hashCode + " index == " + index);
				if(index < 0 ){
					index = 0;
				}
				return index;
			}
	   });
    System.out.println("mapRdd2 partitioner:" + mapRdd2.partitioner());
    System.out.println("mapRdd2 :" + mapRdd2.getNumPartitions());
    System.out.println("mapRdd2 :" + mapRdd2.glom().collect());
/*      
      mapRdd.reduceByKey(
              new Partitioner() {    
		    	      @Override    
		    	      public int numPartitions() {return 2;}    

		    	      @Override    
		    	      public int getPartition(Object o) {
		    	    	  System.out.println("getPartition() " + o.toString());
		    	          return (o.toString()).hashCode() % numPartitions();    
		    	      }
		    	},
		    	new Function2<Integer, Integer, Integer>() {
		    	    @Override
		    	    public Integer call(Integer v1, Integer v2) throws Exception {    
		    	        return v1 + v2;
		    	    }
		        });
/*
      JavaPairRDD<String, Integer> mapRdd2 =  mapRdd.partitionBy(
    		  new Partitioner() {   
		      @Override    
		      public int numPartitions() {return 4;}    
	
		      @Override    
		      public int getPartition(Object o) {
		    	  int index = (o.toString()).hashCode() % numPartitions();
		    	  if(index < 0){
		    		  index = 0;
		    	  }
		    	  return index;  
		      }
	  });
      System.out.println(mapRdd2.getNumPartitions());
      System.out.println(mapRdd2.glom().collect());
      
      JavaPairRDD<String, Integer> mapRdd3 =  mapRdd.partitionBy(
    		  new HashPartitioner(4)
	  );
      System.out.println(mapRdd3.getNumPartitions());
      System.out.println(mapRdd3.glom().collect());      
      
      System.out.println(mapRdd3.partitioner());
      */
    }
}
