package learm.forclass.testclass;

import learm.forclass.utils.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * Created by zhoujun on 17-10-12.
 */
public class TestClass {

    public static void main(String[] args) throws IOException {
//        HBaseUtils.dropTable("table1");
//        HBaseUtils.createTable("table1", "f1,f2");


        filter();
    }

    public static void filter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("table1"));

        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("f2"),
                Bytes.toBytes("c2"),
                CompareFilter.CompareOp.NOT_EQUAL,
                new SubstringComparator("3"));
        Scan scan = new Scan();

        FilterList filterList = new FilterList();
        filterList.addFilter(filter);

        PageFilter pageFilter = new PageFilter(5l);
        filterList.addFilter(pageFilter);

        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(result -> {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength()));
            }
        });

    }

}
