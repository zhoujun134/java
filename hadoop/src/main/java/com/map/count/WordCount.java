package com.map.count;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		conf.set("dfs.blocksize", "67108864"); //67108864
		//配置作业名
		@SuppressWarnings("deprecation") Job job = new Job(conf,"wordcount");
		//配置作业的各个类
		job.setJar("/home/hadoop/workspace/WordCount.jar");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setMinInputSplitSize(job, 33554432l);
		FileInputFormat.setMaxInputSplitSize(job, 67108864l);
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/DD/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/DD/output"));

		System.out.println("运行结束！ ");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

//继承Mapper接口，设置map的输入类型为<Object,Text>
//输出类型为<Text,IntWritable>
class TokenizerMapper
          extends Mapper<Object,Text,Text,IntWritable> {
	//one表示单词出现一次
	private final IntWritable one = new IntWritable(1);
	//word用于存储切下来的单词
	private Text word = new Text();
	
	public void map(Object key, Text value, Context context ) throws IOException,InterruptedException{
		StringTokenizer itr = new StringTokenizer(value.toString());//对输入的行切词
		while(itr.hasMoreTokens()){
			word.set(itr.nextToken());//切下的单词存入word
			context.write(word, one);
		}
	}
}

//继承Reduce接口，设置Reduce的接入类型为<Text,IntWritable>
//输出类型为<Text,IntWritable>

class IntSumReducer extends Reducer<Text, IntWritable, Text,IntWritable> {
	//result记录单词的频数
	private IntWritable result = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int sum = 0;
		//对获取的<key,value-list>计算value的和
		for(IntWritable val : values){
			sum += val.get();
		}
		//将频数设置到result中
		result.set(sum);
		//收集结果
		context.write(key, result);
	}
}