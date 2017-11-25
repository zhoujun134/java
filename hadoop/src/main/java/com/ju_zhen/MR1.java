package com.ju_zhen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MR1 {

    public int run() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String hdfs = "hdfs://192.168.33.128:9000";
        conf.set("fs.defaultFS", hdfs);

        //创建一个Job实例
        Job job = Job.getInstance(conf,"HP1");
        //设置Job的主类
        job.setJarByClass(MR1.class);
        //设置Job的 Mapper 类和 Reducer 类
        job.setMapperClass(HDMapper.class);
        job.setReducerClass(HDReducer.class);

        //设置 Mappe r的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置 Reducer 的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(conf);
        //设置输入和输出路径
        String inpath = "/text/input/input_right.txt";
        Path inputPath = new Path(inpath);
        if(fs.exists(inputPath)) {
            FileInputFormat.addInputPath(job, inputPath);
        }
        String outpath = "/text/output/";
        Path outPath = new Path(outpath);
        fs.delete(outPath, true);

        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true) ? 1 : -1;

    }


    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        int result = new MR1().run();
        if(result > 0) System.out.println(" 运行成功！ ");
        else System.out.println("运行失败！");
    }

}
