package com.learn.book;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class STjion {
	
	public static int time = 0;
	/**
	 * Map将输入分割成child和parent，然后正序输出一次作为右表，反序输出一次作为左表，需要注意
	 * 的是在输出的value中必须加上左右表区别标志
	 * */
	public static class Map extends Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String childname = new String();
			String parentname = new String();
			String relationType = new String();
			String line = new String();
			int i = 0;
			while(line.charAt(i) != ' '){
				i++;
			}
			String[] values = {line.substring(0, i),line.substring(i+1)};
			if(values[0].compareTo("child") != 0){
				childname = values[0];
				parentname = values[1];
				relationType = "1";  //左右表的区别标志
				context.write(new Text(values[1]), new Text(relationType + "+" +
				                  childname + "+" + parentname));
				//左表
				relationType = "2";
				context.write(new Text(values[0]), new Text(relationType +
						"+" + childname + "+" + parentname));
				//右表
			}
		}
	}
	
	public static class Reduce extends Reducer< Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			
			if(time == 0){
				context.write(new Text("grandchild"), new Text("grandparent"));
				time++;
			}
			int grandchildnum = 0;
			String grandchild [] = new String[10];
			int grandparentnum = 0;
			String grandparent[] = new String[10];
			Iterator ite = values.iterator();
			while(ite.hasNext()){
				String record = ite.next().toString();
				int len = record.length();
				int i = 2;
				if(len == 0) continue;
				char relationType = record.charAt(0);
				String childname = new String();
				String parentname = new String();
				//获取value-list中的value的child
				while(record.charAt(i) != '+'){
					childname = childname + record.charAt(i);
					i++;
				}
				//获取value-list中的value的parent
				while(record.charAt(i) != '+'){
					parentname = parentname + record.charAt(i);
					i++;
				}
				
			}
		}
	}
	public static void main(String[] args) {
	}

}
