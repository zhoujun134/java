package learm.forclass.testclass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ScanDemo {
    public static void main(String[] args){
        Configuration conf = HBaseConfiguration.create();
        Table table = null;
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("t1"));
            Scan scan = new Scan();

            scan.setStartRow(Bytes.toBytes("3"));
            scan.setStopRow(Bytes.toBytes("6"));

            ResultScanner resultScanner = table.getScanner(scan);
            resultScanner.forEach(result -> System.out.println(
                    "Scan: " + Bytes.toString(result.getRow()) + " is in class "
                            + Bytes.toString(result.getValue(Bytes.toBytes("f"),
                                             Bytes.toBytes("name")))
            ));

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
