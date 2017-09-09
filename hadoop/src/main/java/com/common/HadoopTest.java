package com.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;


public class HadoopTest extends Configured implements Tool {

    private static double count = 0;

    public static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        private Text word = new Text();
        private LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key,Text value,Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException,InterruptedException{
            System.out.println("line pos:" + key.toString());
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreElements()) {
                count ++;
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class CountReducer extends Reducer<Text, LongWritable, Text, DoubleWritable>{
        private DoubleWritable result = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                              Reducer<Text, LongWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(LongWritable v : values){
                sum += v.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    static FileSystem fs = null;
    static Configuration conf=null;
    public static void init() throws Exception{
        //读取classpath下的xxx-site.xml 配置文件，并解析其内容，封装到conf对象中  
        conf = new Configuration();
        //也可以在代码中对conf中的配置信息进行手动设置，会覆盖掉配置文件中的读取的值  
        conf.set("fs.defaultFS", "hdfs://192.168.27.132:9000/");
        //根据配置信息，去获取一个具体文件系统的客户端操作实例对象  
        fs = FileSystem.get(new URI("hdfs://192.168.27.132:9000/"),conf,"zhoujun");
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(),"WordCount");
        job.setJarByClass(HadoopTest.class);
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        Path in = new Path("hdfs://192.168.27.132:9000/testSpark/testIn.txt");
        if(fs.exists(in)){
            FileInputFormat.addInputPath(job, in);
        }else{
            System.out.println("输入文件不存在！");
        }
        Path os = new Path("hdfs://192.168.27.132:9000/Wordout");
        int flage = 0;
        if(fs.exists(os)){
            System.out.println("输出文件已经存在！重新新建路径！");
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
        int res = ToolRunner.run(new HadoopTest(), args);
        System.exit(res);
    }
}  