package com.base.put;

import com.utils.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 缓存控制的写入数据
 * org.apache.hadoop.hbase.client.BufferedMutator主要用来对HBase的单个表进行操作。
 * 它和Put类的作用差不多，但是主要用来实现批量的异步写操作。BufferedMutator替换了HTable的setAutoFlush(false)的作用。
 * 可以从Connection的实例中获取BufferedMutator的实例。
 * 在使用完成后需要调用close()方法关闭连接。对BufferedMutator进行配置需要通过BufferedMutatorParams完成。
 * MapReduce Job的是BufferedMutator使用的典型场景。MapReduce作业需要批量写入，但是无法找到恰当的点执行flush。
 * BufferedMutator接收MapReduce作业发送来的Put数据后，
 * 会根据某些因素（比如接收的Put数据的总量）启发式地执行Batch Put操作，
 * 且会异步的提交Batch Put请求，这样MapReduce作业的执行也不会被打断。
 * BufferedMutator也可以用在一些特殊的情况上。MapReduce作业的每个线程将会拥有一个独立的BufferedMutator对象。
 * 一个独立的BufferedMutator也可以用在大容量的在线系统上来执行批量Put操作，
 * 但是这时需要注意一些极端情况比如JVM异常或机器故障，此时有可能造成数据丢失。
 */
public class PutWriteBufferExample1 {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HBaseHelper.ZK_CLIENT_PORT_KEY,HBaseHelper.getZkPortValue());
    conf.set(HBaseHelper.ZK_QUORUM_KEY, HBaseHelper.getZkValue());
    conf.set(HBaseHelper.HBASE_KEY, HBaseHelper.getHbaseValue());

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    TableName name = TableName.valueOf("testtable");
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(name);

    BufferedMutator mutator = connection.getBufferedMutator(name);

    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1"));
    mutator.mutate(put1);

    Put put2 = new Put(Bytes.toBytes("row2"));
    put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val2"));
    mutator.mutate(put2);

    Put put3 = new Put(Bytes.toBytes("row3"));
    put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val3"));
    mutator.mutate(put3);

    Get get = new Get(Bytes.toBytes("row1"));
    Result res1 = table.get(get);
    System.out.println("Result: " + res1);

    mutator.flush();

    Result res2 = table.get(get);
    System.out.println("Result: " + res2);

    mutator.close();
    table.close();
    connection.close();
    helper.close();
  }
}
