package com.base.get;

import com.utils.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetCheckExistenceExample {

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

    List<Put> puts = new ArrayList<Put>();
    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1"));
    puts.add(put1);
    Put put2 = new Put(Bytes.toBytes("row2"));
    put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val2"));
    puts.add(put2);
    Put put3 = new Put(Bytes.toBytes("row2"));
    put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
      Bytes.toBytes("val3"));
    puts.add(put3);

    table.put(puts); /** 批量添加到数据库中*/

    Get get1 = new Get(Bytes.toBytes("row2"));
    get1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
    get1.setCheckExistenceOnly(true);
    Result result1 = table.get(get1); /** 获取一行的数据*/

    byte[] val = result1.getValue(Bytes.toBytes("colfam1"),
      Bytes.toBytes("qual1"));

    System.out.println("Get 1 Exists: " + result1.getExists());
    System.out.println("Get 1 Size: " + result1.size()); /** 打印返回的长度*/
    System.out.println("Get 1 Value: " + Bytes.toString(val));

    Get get2 = new Get(Bytes.toBytes("row2"));
    get2.addFamily(Bytes.toBytes("colfam1")); /** 添加列族元素*/
    get2.setCheckExistenceOnly(true);
    Result result2 = table.get(get2);

    System.out.println("Get 2 Exists: " + result2.getExists());
    System.out.println("Get 2 Size: " + result2.size());

    Get get3 = new Get(Bytes.toBytes("row2"));
    get3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual9999"));/** 获取一个不存在的列*/
    get3.setCheckExistenceOnly(true);
    Result result3 = table.get(get3);

    System.out.println("Get 3 Exists: " + result3.getExists());
    System.out.println("Get 3 Size: " + result3.size());

    Get get4 = new Get(Bytes.toBytes("row2"));
    get4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual9999")); /** 一个存在一个不存在的列族*/
    get4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
    get4.setCheckExistenceOnly(true);
    Result result4 = table.get(get4);

    System.out.println("Get 4 Exists: " + result4.getExists());  // true
    System.out.println("Get 4 Size: " + result4.size());

    table.close();
    connection.close();
    helper.close();
  }
}
