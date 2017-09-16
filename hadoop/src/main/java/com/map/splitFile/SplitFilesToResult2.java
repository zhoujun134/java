package com.map.splitFile;
import java.io.IOException;  
import java.net.URI;  
import java.net.URISyntaxException;  
import java.util.regex.Pattern;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
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

public class SplitFilesToResult2 extends Configured {
  
    @SuppressWarnings("deprecation")
	public static void main(String[] args) {  
        String in = "/SplitFilesToResult/input";  
        String out = "/SplitFilesToResult/output2";  
          
        Job job;
        try {  
            //删除hdfs目录  
        	SplitFilesToResult wc2 = new SplitFilesToResult();  
            wc2.removeDir(out);  
              
            job = new Job(new Configuration(), "split Job");
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(mapperString1.class);
            job.setReducerClass(reduceStatistics1.class);
            job.setPartitionerClass(MyPartitioner.class);
            
            FileInputFormat.addInputPath(job, new Path(in));
            FileOutputFormat.setOutputPath(job, new Path(out));
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
class mapperString1 extends Mapper<LongWritable, Text, Text, Text> {
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
class reduceStatistics1 extends Reducer<Text, Text, Text, Text> {
    @Override  
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
    	for(Text t: values){
        	context.write(key, new Text(t.toString()));
    	}
          
    }
}  