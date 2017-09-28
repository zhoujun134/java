package learm.forclass.testclass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class GetDemo {
    public static void main(String[] args){
        Configuration conf = HBaseConfiguration.create();
        Table table = null;
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("a3"));
            Get get = new Get(Bytes.toBytes("r1"));
            Result result = table.get(get);
            byte[] value = result.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("name"));
            System.out.println(" r1: cf1:namme:    value = " +Bytes.toString(value));

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                assert table != null;
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
