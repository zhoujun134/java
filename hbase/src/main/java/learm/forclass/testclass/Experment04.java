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
 * 实验四
 */
public class Experment04 {

    /**
     * 获取配置信息
     */
    private static Configuration conf = HBaseConfiguration.create();


    /**
     * 统计错误信息的条数
     */
    private static double errorCount = 0;

    /**
     * 统计 fatal 信息的条数
     */
    private static double fatalCount = 0;

    /**
     * 统计总信息的条数
     */
    private static double total = 0;

    public static void main(String[] args) throws IOException {
        usebatch();
    }


    /**
     * 根据制定的条件查询数据，返回获取到的条数
     * @param level 要过滤的值
     * @return
     * @throws IOException
     */
    private static double filterData(String level) throws IOException {
        double[] resultCount = {0}; // 结果
        /**
         * 创建连接实例，获取表实例
         */
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("classtable"));

        /**
         * 添加过滤器，指定过滤条件
         */
        Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator(level));

        Scan scan = new Scan();
        /**
         * 如果不为 * 添加 filter 到 scan 中，为 * 表示查询所有数据
         */
        if(!level.equals("*")) scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);

        /** 统计获取到的行 */
        scanner.forEach(result -> {
               resultCount[0]++;
        });

        /** 关闭连接*/
        table.close();
        connection.close();
        return resultCount[0];
    }


    /**
     * 计算 （error + fatal）/total 的值
     * @return
     * @throws IOException
     */
    private static double compute() throws IOException{
        double result  = 0;
        /** 获取对应信息的数量 */
        errorCount = filterData("ERROR");
        fatalCount = filterData("FATAL");
        total = filterData("*");
        System.out.println(" total: " + total +"  error: " + errorCount + " fatal: " + fatalCount );
        result = (errorCount + fatalCount)/total;
        System.out.println("error+fatal/total = " + result);
        return result;
    }

    /**
     * 更新数据库中行中的列限定符为：class 的 value 的值，去掉后面的 “ ：”
     * @return
     * @throws IOException
     */
    private static void updateData() throws IOException {
        /**
         * 创建连接实例，获取表实例
         */
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("classtable"));

        /**
         * 添加过滤器，指定过滤条件
         */
        Filter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("class"));
        Scan scan = new Scan();
        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        /** 保存每行需要更新数据的 Put 实例 */
        ArrayList<Put> arrayList = new ArrayList<>();

        /** 遍历添加实例 */
        scanner.forEach(result -> {
            Arrays.stream(result.rawCells()).forEach(cell -> {
                Put put = new Put(result.getRow());
                /** 去掉 ’：‘*/
                String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength()).split(":")[0];
                put.addColumn(Bytes.toBytes("f"),
                        Bytes.toBytes("class"),
                        Bytes.toBytes(val));
                arrayList.add(put);
            });
        });

        /** 更新数据 */
        table.put(arrayList);
        /** 关闭连接*/
        table.close();
        connection.close();
    }


    /**
     * 使用 hbase 的 batch 操作！
     * @throws IOException
     */
    public static void usebatch() throws IOException {
        /**
         * 创建连接实例，获取表实例
         */
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("test"));

        /** 装载 batch 的 list */
        List<Row> batch = new ArrayList<Row>();

        /** 添加对应的曾 删 查 实例到 batch 列表中*/
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

        /** 获取返回后的结果 */
        Object[] results = new Object[batch.size()];
        try {
            /** 执行对应的 batch 操作*/
            table.batch(batch, results);
        } catch (Exception e) {
            System.err.println("Error: " + e);
        }

        /** 打印结果 */
        for (int i = 0; i < results.length; i++) {
            System.out.println("Result[" + i + "]: type = " +
                    results[i].getClass().getSimpleName() + "; " + results[i].toString());
        }
        /** 关闭连接*/
        table.close();
        connection.close();
    }
}

