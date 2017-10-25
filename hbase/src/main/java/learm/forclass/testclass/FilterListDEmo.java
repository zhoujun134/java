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
import java.util.ArrayList;

/**
 * Created by zhoujun on 17-10-16.
 */
public class FilterListDEmo {
    public static void main(String[] args) throws IOException {

//        HBaseUtils.createTable("table2","cf1,cf2");
//        HBaseUtils.putDataH("table2","001","cf1","c2","1");;
//        HBaseUtils.putDataH("table2","001","cf1","c3","2");;
//        HBaseUtils.putDataH("table2","002","cf1","c1","3");;
//        HBaseUtils.putDataH("table2","005","cf1","c1","2");;
//        HBaseUtils.putDataH("table2","005","cf1","c2","1");


        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("table2"));




        Filter filter1 = new QualifierFilter(CompareFilter.CompareOp.NOT_EQUAL,
                new BinaryComparator(Bytes.toBytes("c2")));

        Filter filter2 = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL,
                new BinaryComparator(Bytes.toBytes("3")));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                new ArrayList<Filter>(){{
                    this.add(filter1);
                    this.add(filter2);
        }});

        Scan scan = new Scan();
        scan.setFilter(filterList);

        ResultScanner results = table.getScanner(scan);
        results.forEach(result -> {

            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength()));
            }
        });
    }
}
