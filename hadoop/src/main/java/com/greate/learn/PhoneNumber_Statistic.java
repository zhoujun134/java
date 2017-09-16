package com.greate.learn;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PhoneNumber_Statistic extends Configured implements Tool {
	public static void main (String[] args) throws Exception{
			ToolRunner.run(new PhoneNumber_Statistic(), args);
	}
	public int run(String[] arg0) throws Exception{
		Configuration conf = getConf();
	    @SuppressWarnings("deprecation") Job job = new Job(conf);
		job.setJarByClass(getClass());
		
		FileInputFormat.setInputPaths(job, new Path("/PhoneNumber_Statistics/input/"));
		FileOutputFormat.setOutputPath(job, new Path("/PhoneNumber_Statistics/output/"));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(numberMap.class);
		job.setReducerClass(numberReduce.class);
		job.waitForCompletion(true);

		return 0;
	}
}

class numberMap extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context)
throws IOException,InterruptedException{
			String[] list = value.toString().split(" "); 		
			String keyy = list[1];
			String valuee = list[0];
			context.write(new Text(keyy), new Text(valuee));
		}
}

class numberReduce extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException,InterruptedException{ 
		String valuee; 
        String out = "";  
        for(Text value:values){
        	valuee  = value.toString() + " | "; 
        	out +=valuee; 
        }
        context.write(key,new Text(out));
	}
}
