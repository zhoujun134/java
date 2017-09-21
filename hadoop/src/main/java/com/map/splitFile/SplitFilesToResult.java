package com.map.splitFile;
import java.io.IOException;  
import java.net.URI;  
import java.net.URISyntaxException;  
import java.util.regex.Pattern;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SplitFilesToResult extends Configured {
  
    @SuppressWarnings("deprecation")
	public static void main(String[] args) {  
        String in = "/SplitFilesToResult/input";  
        String out = "/SplitFilesToResult/output";  
          
        Job job;
        try {  
            //如果输出文件存在，则删除hdfs目录  
        	SplitFilesToResult wc2 = new SplitFilesToResult();
            wc2.removeDir(out);  
              
            job = new Job(new Configuration(), "split Job");
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(mapperString.class);
            job.setReducerClass(reduceStatistics.class);
              
            //自定义附加的输出文件  
            MultipleOutputs.addNamedOutput(job,"INFO",TextOutputFormat.class,Text.class,Text.class);
            MultipleOutputs.addNamedOutput(job,"ERROR",TextOutputFormat.class,Text.class,Text.class);
            MultipleOutputs.addNamedOutput(job,"WARN",TextOutputFormat.class,Text.class,Text.class);
            MultipleOutputs.addNamedOutput(job,"OTHER",TextOutputFormat.class,Text.class,Text.class);
            
            FileInputFormat.addInputPath(job, new Path(in));
            FileOutputFormat.setOutputPath(job, new Path(out));
            // 去掉job设置outputFormatClass，改为通过LazyOutputFormat设置
            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
            job.waitForCompletion(true);  

        } catch (IOException e) {  
            e.printStackTrace();  
        } catch (URISyntaxException e) {  
            e.printStackTrace();  
        } catch (ClassNotFoundException e) {  
            e.printStackTrace();  
        } catch (InterruptedException e) {  
            e.printStackTrace();  
        }  
    }  
      
    @SuppressWarnings("deprecation")
	public void removeDir(String filePath) throws IOException, URISyntaxException{  
        String url = "hdfs://localhost:9000";  
        FileSystem fs  = FileSystem.get(new URI(url), new Configuration());
        fs.delete(new Path(filePath));
    }  
}

/** 
 * 重写maptask使用的map方法  
 * @author nange 
 * 
 */  
class mapperString extends Mapper<LongWritable, Text, Text, Text> {
    //设置正则表达式的编译表达形式  
    public static Pattern PATTERN = Pattern.compile(" ");
    @Override  
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {  
          
        String[] words = PATTERN.split(value.toString());  
        System.out.println("********" + value.toString());
       if(words.length >= 2){
    	   if(words.length == 2){
        	   context.write(new Text("ERROR"), new Text(value.toString()));
    	   }else if(words[0].equals("at")){
        	   context.write(new Text("ERROR"), new Text(value.toString()));
           }else{
    	       context.write(new Text(words[2]), new Text(value.toString()));
    	   }
       }else
           context.write(new Text("OTHER"), new Text(value.toString()));
    }  
}

/** 
 * 对单词做统计 
 * @author nange 
 * 
 */  
class reduceStatistics extends Reducer<Text, Text, Text, Text> {
  
    //将结果输出到多个文件或多个文件夹  
    private MultipleOutputs<Text,Text> mos;
    //创建MultipleOutputs对象  
    protected void setup(Context context) throws IOException,InterruptedException {
        mos = new MultipleOutputs<Text, Text>(context);
     }  
      
    @Override  
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
    	for(Text t: values){
            //使用MultipleOutputs对象输出数据  
            if(key.toString().equals("INFO")){  
                mos.write("INFO", "", t);  
            }else if(key.toString().equals("ERROR")){  
                mos.write("ERROR", "", t);  
            }else if(key.toString().equals("WARN")){
                mos.write("WARN", "", t, "WARN");  
            }else{
            	mos.write("OTHER", "", t);
            }
    	}
          
    }  
      
    //关闭MultipleOutputs对象  
    protected void cleanup(Context context) throws IOException,InterruptedException {
        mos.close();  
    }  
}  