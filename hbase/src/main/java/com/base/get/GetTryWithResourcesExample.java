package com.base.get;

// cc GetTryWithResourcesExample Example application retrieving data from HBase using a Java 7 construct

import com.utils.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class GetTryWithResourcesExample {

  public static void main(String[] args) throws IOException {
    // vv GetTryWithResourcesExample
    Configuration conf = HBaseConfiguration.create(); // co GetTryWithResourcesExample-1-CreateConf Create the configuration.

    // ^^ GetTryWithResourcesExample
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    if (!helper.existsTable("testtable")) {
      helper.createTable("testtable", "colfam1");
    }
    // vv GetTryWithResourcesExample
    try (
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf("testtable")); // co GetTryWithResourcesExample-2-NewTable Instantiate a new table reference in "try" block.
    ) {
      Get get = new Get(Bytes.toBytes("row1"));
      get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
      Result result = table.get(get);
      byte[] val = result.getValue(Bytes.toBytes("colfam1"),
        Bytes.toBytes("qual1"));
      System.out.println("Value: " + Bytes.toString(val));
    }
    helper.close();
  }
}
