package learm.forclass.maprdeuce;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhoujun on 17-10-17.
 */
public class HbaseMRDemo{

    static class Mapper extends TableMapper<Text, Text>{

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {


            super.map(key, value, context);
        }
    }

}
