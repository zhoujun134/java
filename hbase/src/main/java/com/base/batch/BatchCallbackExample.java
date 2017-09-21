package com.base.batch;


import com.utils.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 运用批量操作，batch
 */
public class BatchCallbackExample {

    private final static byte[] ROW1 = Bytes.toBytes("row1");
    private final static byte[] ROW2 = Bytes.toBytes("row2");
    private final static byte[] COLFAM1 = Bytes.toBytes("colfam1");
    private final static byte[] COLFAM2 = Bytes.toBytes("colfam2");
    private final static byte[] QUAL1 = Bytes.toBytes("qual1");
    private final static byte[] QUAL2 = Bytes.toBytes("qual2");

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HBaseHelper.ZK_CLIENT_PORT_KEY,HBaseHelper.getZkPortValue());
    conf.set(HBaseHelper.ZK_QUORUM_KEY, HBaseHelper.getZkValue());
    conf.set(HBaseHelper.HBASE_KEY, HBaseHelper.getHbaseValue());

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    helper.put("testtable",
      new String[] { "row1" },
      new String[] { "colfam1" },
      new String[] { "qual1", "qual2", "qual3" },
      new long[] { 1, 2, 3 },
      new String[] { "val1", "val2", "val3" });
    System.out.println("Before batch call...");
    helper.dump("testtable", new String[] { "row1", "row2" }, null, null);

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    List<Row> batch = new ArrayList<Row>();

    Put put = new Put(ROW2);
    put.addColumn(COLFAM2, QUAL1, 4, Bytes.toBytes("val5"));
    batch.add(put); // 添加一个put实例

    Get get1 = new Get(ROW1);
    get1.addColumn(COLFAM1, QUAL1); // 添加一个get实例
    batch.add(get1);

    Delete delete = new Delete(ROW1);
    delete.addColumns(COLFAM1, QUAL2); // 添加一个delete实例
    batch.add(delete);

    Get get2 = new Get(ROW2);
    get2.addFamily(Bytes.toBytes("BOGUS")); // co BatchCallbackExample-6-AddBogus Add a Get instance that will fail.
    batch.add(get2);

    Object[] results = new Object[batch.size()]; // co BatchCallbackExample-7-CreateResult Create result array.
    try {
      table.batchCallback(batch, results, new Batch.Callback<Result>() {
        @Override
        public void update(byte[] region, byte[] row, Result result) {
          System.out.println("Received callback for row[" + 
            Bytes.toString(row) + "] -> " + result);
        }
      });
    } catch (Exception e) {
      System.err.println("Error: " + e);
    }

    for (int i = 0; i < results.length; i++) {
      System.out.println("Result[" + i + "]: type = " + // co BatchCallbackExample-9-Dump Print all results and class types.
        results[i].getClass().getSimpleName() + "; " + results[i]);
    }
    // ^^ BatchCallbackExample
    table.close();
    connection.close();
    System.out.println("After batch call...");
    helper.dump("testtable", new String[]{"row1", "row2"}, null, null);
    helper.close();
  }
}
