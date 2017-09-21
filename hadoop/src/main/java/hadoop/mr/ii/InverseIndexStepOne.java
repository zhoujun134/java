package hadoop.mr.ii;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 倒排索引步骤一job
 * @author duanhaitao@itcast.cn
 *
 */
public class InverseIndexStepOne {
	
	
	
	public static class StepOneMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			//拿到一行数据
			String line = value.toString();
			//切分出各个单词
			String[] fields = StringUtils.split(line, " ");
			//获取这一行数据所在的文件切片
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			//从文件切片中获取文件名
			String fileName = inputSplit.getPath().getName();
			for(String field:fields){
				//封装kv输出  ，  k ：  hello-->a.txt     v:  1
				context.write(new Text(field+"-->"+fileName), new LongWritable(1));
			}
		}
	}
	public static class StepOneReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		// <hello-->a.txt,{1,1,1....}>
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long counter = 0;
			for(LongWritable value:values){
				counter += value.get();
			}
			context.write(key, new LongWritable(counter));
		}
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(InverseIndexStepOne.class);
		
		job.setMapperClass(StepOneMapper.class);
		job.setReducerClass(StepOneReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		//检查一下参数所指定的输出路径是否存在，如果已存在，先删除
		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(output)){
			fs.delete(output, true);
		}
		
		FileOutputFormat.setOutputPath(job, output);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	}

}