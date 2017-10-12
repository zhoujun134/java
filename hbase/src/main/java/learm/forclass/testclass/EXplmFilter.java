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
import java.util.Arrays;
import java.util.List;

/**
 * Created by zhoujun on 17-10-12.
 */
public class EXplmFilter {

    /**
     * 获取配置信息
     */
    private static Configuration conf = HBaseConfiguration.create();


    private static double errorCount = 0;

    private static double fatalCount = 0;

    private static double total = 0;

    public static void main(String[] args) throws IOException {

        usebatch();
    }


    private static double filterData(String level) throws IOException {

        double[] resultCount = {0};
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("classtable"));

        Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator(level));

        Scan scan = new Scan();
        if(!level.equals("*")) scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);

        scanner.forEach(result -> {
               resultCount[0]++;
        });

        return resultCount[0];
    }


    private static double compute() throws IOException{
        double result  = 0;
        errorCount = filterData("ERROR");
        fatalCount = filterData("FATAL");
        total = filterData("*");
        System.out.println(" total: " + total +"  error: " + errorCount + " fatal: " + fatalCount );
        result = (errorCount + fatalCount)/total;
        System.out.println("error+fatal/total = " + result);
        return result;
    }

    private static double updateData() throws IOException {

        double[] resultCount = {0};
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("classtable"));

        Filter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("class"));

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        ArrayList<Put> arrayList = new ArrayList<>();

        scanner.forEach(result -> {
            Arrays.stream(result.rawCells()).forEach(cell -> {
                Put put = new Put(result.getRow());
                String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength()).split(":")[0];
                put.addColumn(Bytes.toBytes("f"),
                        Bytes.toBytes("class"),
                        Bytes.toBytes(val));
                arrayList.add(put);
            });
        });

        table.put(arrayList);
        table.close();

        return resultCount[0];
    }


    public static void usebatch() throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("test"));

        List<Row> batch = new ArrayList<Row>();

        Put put = new Put(Bytes.toBytes("r-1"));
        put.addColumn(Bytes.toBytes("f"),
                Bytes.toBytes("qua-xx"),
                Bytes.toBytes("val-xx"));
        batch.add(put);

        Get get1 = new Get(Bytes.toBytes("r-2"));
        get1.addColumn(Bytes.toBytes("f"),
                Bytes.toBytes("qua-2"));
        batch.add(get1);

        Delete delete = new Delete(Bytes.toBytes("r-0"));
        delete.addColumns(Bytes.toBytes("f"),
                Bytes.toBytes("qua-0"));
        batch.add(delete);

        Get get2 = new Get(Bytes.toBytes("r-9"));
        get2.addFamily(Bytes.toBytes("f"));
        batch.add(get2);

        Object[] results = new Object[batch.size()];
        try {

            table.batch(batch, results);

        } catch (Exception e) {
            System.err.println("Error: " + e);
        }

        for (int i = 0; i < results.length; i++) {
            System.out.println("Result[" + i + "]: type = " +
                    results[i].getClass().getSimpleName() + "; " + results[i].toString());
        }
        table.close();
        connection.close();
    }
}

