package com.ju_zhen;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HDMapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
     * 输出的 key
     */
    private Text outKey = new Text();
    /**
     * 输出的 value
     */
    private Text outValue = new Text();

    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] rowAndLine = value.toString().split("\t");
        //矩阵的行号
        String row = rowAndLine[0];
        String[] lines = rowAndLine[1].split(",");

        //1_0,2_3,3_-1,4_2,5_-3
        for (String line : lines) {
            String column = line.split("_")[0];
            String valueStr = line.split("_")[1];
            //key 为列号， value： 行号_值
            outKey.set(column);
            outValue.set(row + "_" + valueStr);
            context.write(outKey, outValue);
        }


    }
}
