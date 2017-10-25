package com;

import com.learn.book.Digital_To_Heavy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by zhoujun on 17-10-18.
 */
public class TestHadoop {

    /**
     * map将输入中的value复制到输出数据的key上，并且直接输出
     * */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] values = value.toString().split(" ");
            for(String s: values){
                System.out.println("info: " + s);
            }
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
        Job job = new Job(conf, "Data Deduplication");
        job.setJarByClass(Digital_To_Heavy.class);
        job.setMapperClass(Digital_To_Heavy.Map.class);
        job.setReducerClass(Digital_To_Heavy.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://zhoujun:9000/test/*"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://zhoujun:9000/test/out"));
        System.out.println("运行完成");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
