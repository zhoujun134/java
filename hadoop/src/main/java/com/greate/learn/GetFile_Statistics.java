package com.greate.learn;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GetFile_Statistics extends Configured implements Tool {
	
	public static class CountMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text one = new Text(1+"");
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException,InterruptedException{
					System.out.println("line pos:" + key.toString());
					String line = value.toString();
					String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
					StringTokenizer tokenizer = new StringTokenizer(line);
					while (tokenizer.hasMoreElements()) {
						word.set(tokenizer.nextToken()+" :  "+fileName);
						context.write(word, one);
					}
				}
	}

	public static class Combiner extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(Text v : values){
				sum += Integer.parseInt(v.toString());
			}
			System.out.println("sum:" + sum);
			String[] valueString = key.toString().split(" : ");
			context.write(new Text(valueString[0]), new Text(valueString[1]+":" + sum));
		}
	}

	public static class CountReducer extends Reducer<Text, Text, Text, Text> {
		static String beforeKey = "";
		static String beforeValue ="";
		@Override
		protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			String key2 = key.toString();
			String value = "";
			for(Text text: values){
				value = text.toString();
				if(key2.equals(beforeKey)){
					beforeKey = key2;
					beforeValue = beforeValue +";"+value;
				}else{
					beforeKey =  key2;
					beforeValue = value;
				}
			}
			
			context.write(new Text(beforeKey), new Text(beforeValue));
		}
	}
	
	static FileSystem fs = null;
	static Configuration conf=null;
	public static void init() throws Exception{
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000/");
		fs = FileSystem.get(new URI("hdfs://localhost:9000/"),conf,"hadoop");
	}
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(),"ResultSplit"
				+ "");
		job.setJarByClass(GetFile_Statistics.class);
		
		job.setMapperClass(CountMapper.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(CountReducer.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path in = new Path("/GetFile_Statistics/input");
		if(fs.exists(in)){
			FileInputFormat.addInputPath(job, in);
		}else{
			System.out.println("文件夹不存在，需要创建！");
		}
		Path os = new Path("/GetFile_Statistics/output");
		int flage = 0;
		if(fs.exists(os)){
			System.out.println("文件夹存在！不再创建！");
			 fs.delete(os, true);  
			 FileOutputFormat.setOutputPath(job, os);
			 flage = job.waitForCompletion(false) ? 0:1;
		}else{
			FileOutputFormat.setOutputPath(job, os);
			flage = job.waitForCompletion(false) ? 0:1;
		}
		return  flage;
	}

	public static void main(String[] args) throws Exception {
		init();
		int res = ToolRunner.run(new GetFile_Statistics(), args);
		System.exit(res);
	}
}
