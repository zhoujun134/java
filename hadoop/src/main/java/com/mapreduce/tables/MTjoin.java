package com.mapreduce.tables;
import java.io.IOException;
import java.util.*;



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

public class MTjoin {
    public static int time = 0;
    /**
     * 在map中先区分输入行属于左表还是右表，然后对两列值进行分割，
     * 保存连接列在key值，剩余列和左右表标志在value中，最后输出
     */
    public static class Map extends Mapper<Object, Text, Text, Text> {
        // 实现map函数
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();// 每行文件
            String relationtype = new String();// 左右表标识
            // 输入文件首行，不处理
            if (line.contains("factoryname") == true
                    || line.contains("addressed") == true) {
                return;
            }
            // 输入的一行预处理文本
            StringTokenizer itr = new StringTokenizer(line);
            String mapkey = new String();
            String mapvalue = new String();
            int i = 0;
            while (itr.hasMoreTokens()) {
               // 先读取一个单词
               String token = itr.nextToken();
                // 判断该地址ID就把存到"values[0]"
                if (token.charAt(0) >= '0' && token.charAt(0) <= '9') {
                    mapkey = token;
                    if (i > 0) {
                        relationtype = "1";
                    } else {
                       relationtype = "2";
                    }
                    continue;
                }
                // 存工厂名
                mapvalue += token + " ";
                i++;
            }
            // 输出左右表
           context.write(new Text(mapkey), new Text(relationtype + "+"+ mapvalue));
        }
    }
 /**
  * reduce解析map输出，将value中数据按照左右表分别保存，
　　* 然后求出笛卡尔积，并输出。
  */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        // 实现reduce函数
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 输出表头
            if (0 == time) {
                context.write(new Text("factoryname"), new Text("addressname"));
                time++;
            }
            int factorynum = 0;
            String[] factory = new String[10];
            int addressnum = 0;
            String[] address = new String[10];
            Iterator ite = values.iterator();
            while (ite.hasNext()) {
                String record = ite.next().toString();
                int len = record.length();
               int i = 2;
                if (0 == len) {
                    continue;
                }
                // 取得左右表标识
                char relationtype = record.charAt(0);
                // 左表
                if ('1' == relationtype) {
                   factory[factorynum] = record.substring(i);
                    factorynum++;
                }
               // 右表
               if ('2' == relationtype) {
                    address[addressnum] = record.substring(i);
                    addressnum++;
                }
            }
            // 求笛卡尔积
            if (0 != factorynum && 0 != addressnum) {
                for (int m = 0; m < factorynum; m++) {
                    for (int n = 0; n < addressnum; n++) {
                        // 输出结果
                        context.write(new Text(factory[m]),
                                new Text(address[n]));
                    }
               }
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 这句话很关键
        conf.set("mapred.job.tracker", "192.168.1.2:9001");
       String[] ioArgs = new String[] { "MTjoin_in", "MTjoin_out" };
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Multiple Table Join <in> <out>");
           System.exit(2);
        }
        Job job = new Job(conf, "Multiple Table Join");
        job.setJarByClass(MTjoin.class);
        // 设置Map和Reduce处理类
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}