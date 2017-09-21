package com.filter.row;

import com.utils.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 基于行 row 的过滤器
 * blocalsize 越大 读数据的速度越快
 *
 */
public class RowFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    //如果存在就删除表
    helper.dropTable("testtable");
    //创建表
    helper.createTable("testtable", "colfam1", "colfam2");
    System.out.println("Adding rows to table...");
    //向表填充数据
    helper.fillTable("testtable", 1, 100, 100, "colfam1", "colfam2");

    //获取表的实例
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    //遍历查询表的数据
    Scan scan = new Scan();
    scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-1"));

    // 创建 RowFilter实例中的过滤条件的 Filter 接口实例 小于等于
    Filter filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
      new BinaryComparator(Bytes.toBytes("row-22")));
    // 添加到 scan 实例中
    scan.setFilter(filter1);
    // 获取结果
    ResultScanner scanner1 = table.getScanner(scan);

    System.out.println("Scanning table #1...");
    scanner1.forEach(System.out::println);
    scanner1.close();

    // 创建 RowFilter实例中的过滤条件的 Filter 接口实例 等于
    Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL,
      new RegexStringComparator(".*-.5"));
    scan.setFilter(filter2);
    ResultScanner scanner2 = table.getScanner(scan);
    System.out.println("Scanning table #2...");
    scanner2.forEach(System.out::println);
    scanner2.close();

    Filter filter3 = new RowFilter(CompareFilter.CompareOp.EQUAL,
      new SubstringComparator("-5"));
    scan.setFilter(filter3);
    ResultScanner scanner3 = table.getScanner(scan);
    System.out.println("Scanning table #3...");

    scanner3.forEach(System.out::println);
    // 关闭连接
    scanner3.close();
  }
}
