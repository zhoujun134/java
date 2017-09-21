package com.learn.book;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 数字去重
 * */
public class Digital_To_Heavy {
	
	/**
	 * map将输入中的value复制到输出数据的key上，并且直接输出
	 * */
	public static class Map extends Mapper<Object, Text, Text, Text> {
		private static Text line = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			line = value;
			context.write(line, new Text(""));
		}
	}
	/**
	 * reduce将输入中的key复制到输出数据的key上，并直接输出
	 * */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			context.write(key, new Text(""));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println(" Usage :wordcount <in> <out>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation") Job job = new Job(conf, "Data Deduplication");
		job.setJarByClass(Digital_To_Heavy.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.out.println("运行完成");
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}

}
