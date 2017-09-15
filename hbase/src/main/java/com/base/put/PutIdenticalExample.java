package com.base.put;

import com.utils.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class PutIdenticalExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HBaseHelper.ZK_CLIENT_PORT_KEY,HBaseHelper.getZkPortValue());
    conf.set(HBaseHelper.ZK_QUORUM_KEY, HBaseHelper.getZkValue());
    conf.set(HBaseHelper.HBASE_KEY, HBaseHelper.getHbaseValue());

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    Put put = new Put(Bytes.toBytes("row1"));
    put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val2"));
    put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1"));
    table.put(put);

    /**
     * 获取刚才插入的数据
     */
    Get get = new Get(Bytes.toBytes("row1"));
    Result result = table.get(get);
    System.out.println("Result: " + result + ", Value: " + Bytes.toString(
      result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))));

    /**
     * 关闭数据流，以及相关连接
     */
    table.close();
    connection.close();
    helper.close();
  }
}
