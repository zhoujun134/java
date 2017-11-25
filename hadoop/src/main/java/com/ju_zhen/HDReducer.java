package com.ju_zhen;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HDReducer extends Reducer<Text, Text,Text, Text>{

    /**
     * 输出的 key
     */
    private Text outKey = new Text();
    /**
     * 输出的 value
     */
    private Text outValue = new Text();

    /**
     *  key 为列号， value： 行号_值
     * @param key 输入的 key
     * @param values 输入的 value
     * @param context 上下文对象
     * @throws IOException 异常1
     * @throws InterruptedException 异常2
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text text: values){
            //text: 行号_值
            sb.append(text).append(",");
        }
        String line = null;
        if(sb.toString().equals(",")){
            line = sb.substring(0, sb.length()-1);
        }
        outKey.set(key);
        assert line != null;
        outValue.set(line);
        context.write(outKey, outValue);
    }
}
