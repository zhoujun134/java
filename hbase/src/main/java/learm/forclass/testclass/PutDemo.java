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
            table = connection.getTable(TableName.valueOf("table1"));

            for(int i=0; i<10; i++){
                Put put = new Put(Bytes.toBytes("r"+i));
                put.addColumn(Bytes.toBytes("f1"),
                        Bytes.toBytes("c2"),
                        Bytes.toBytes(""+i));
                table.put(put);
            }
            System.out.println("插入的数据为：");
            Scan scan = new Scan().addFamily(Bytes.toBytes("f1"));
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
