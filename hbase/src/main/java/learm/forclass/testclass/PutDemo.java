package learm.forclass.testclass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class PutDemo {
    public static void main(String[] args){
        Configuration conf = HBaseConfiguration.create();
        Table table = null;
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("a"));
            Put put = new Put(Bytes.toBytes("row2"));
            put.addColumn(Bytes.toBytes("f"),
                          Bytes.toBytes("name"),
                          Bytes.toBytes("person1"));
            table.put(put);

            System.out.println("插入的数据为：");
            Scan scan = new Scan().addFamily(Bytes.toBytes("f"));
            table.getScanner(scan).forEach(System.out::println);


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
