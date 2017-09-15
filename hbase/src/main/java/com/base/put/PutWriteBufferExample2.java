package com.base.put;

// cc PutWriteBufferExample2 Example using the client-side write buffer

import com.utils.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutWriteBufferExample2 {

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

    List<Mutation> mutations = new ArrayList<Mutation>();

    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val1"));
    mutations.add(put1);

    Put put2 = new Put(Bytes.toBytes("row2"));
    put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val2"));
    mutations.add(put2);

    Put put3 = new Put(Bytes.toBytes("row3"));
    put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
      Bytes.toBytes("val3"));
    mutations.add(put3);

    mutator.mutate(mutations);

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
