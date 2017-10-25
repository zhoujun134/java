package com.filter.row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by zhoujun on 17-10-9.
 */
public class TestFilter {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        String tableName = "testN";
//        HBaseUtils.createTable(tableName,"f1,f2");
//        for(int i=0; i<100; i++){
//            HBaseUtils.putDataH(tableName, "row"+i,"f1","xxxq-"+i,"val"+i);
//        }


        //获取表的实例
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        //遍历查询表的数据
        Scan scan = new Scan();

        // 创建 RowFilter实例中的过滤条件的 Filter 接口实例 小于等于
//        Filter filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
//                new BinaryComparator(Bytes.toBytes("row22")));
//
        Filter filter1 = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(Bytes.toBytes("qua")));

//        SingleColumnValueFilter filter = new SingleColumnValueFilter(
//                Bytes.toBytes("f1"),
//                Bytes.toBytes("qua-5"),
//                CompareFilter.CompareOp.NOT_EQUAL,
//                new BinaryComparator(Bytes.toBytes("val5")));
//        filter.setFilterIfMissing(true);

        scan.setFilter(filter1);

        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(System.out :: println);


    }
}
