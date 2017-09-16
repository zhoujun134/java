package hadoop.mr.flowsort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.itcast.hadoop.mr.flowsum.FlowBean;

public class SortMR {
	
	
	public static class SortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {
		
		//拿到一行数据，切分出各字段，封装为一个flowbean，作为key输出
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			
			String[] fields = StringUtils.split(line, "\t");
			
			String phoneNB = fields[0];
			long u_flow = Long.parseLong(fields[1]);
			long d_flow = Long.parseLong(fields[2]);
			
			context.write(new FlowBean(phoneNB, u_flow, d_flow), NullWritable.get());
			
		}
		
		
	}
	
	
	
	public static class SortReducer extends Reducer<FlowBean, NullWritable, Text, FlowBean> {
		
		@Override
		protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {

			String phoneNB = key.getPhoneNB();
			context.write(new Text(phoneNB), key);
			
		}
		
	}
	
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJar("/home/hadoop/workspace/Word.jar");
		job.setJarByClass(SortMR.class);
		
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path("/Sort/input"));
		FileOutputFormat.setOutputPath(job, new Path("/Sort/output"));
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	}
	
	
	

}