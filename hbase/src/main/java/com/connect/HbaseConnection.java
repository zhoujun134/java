package com.connect;

import com.utils.HBaseUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by zhoujun on 2017/9/9.
 */
public class HbaseConnection {
    /**
     * 连接池对象
     */
    protected static Connection connection;
    private static final String ZK_QUORUM = "hbase.zookeeper.quorum";
    private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
    /**
     * HBase位置
     */
    private static final String HBASE_POS = "192.168.27.132";

    /**
     * ZooKeeper位置
     */
    private static final String ZK_POS = "192.168.27.132";

    /**
     * zookeeper服务端口
     */
    private final static String ZK_PORT_VALUE = "2181";

    /**
     * 静态构造，在调用静态方法时前进行运行
     * 初始化连接对象．
     * */
    static{
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://" + HBASE_POS
                + ":9000/hbase");
        configuration.set(ZK_QUORUM, ZK_POS);
        configuration.set(ZK_CLIENT_PORT, ZK_PORT_VALUE);
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }// 创建连接池
    }

    /**
     * @param tableName 创建一个表 tableName 指定的表名
     * @param seriesStr  以字符串的形式指定表的列族，每个列族以逗号的形式隔开,(例如：＂f1,f2＂两个列族，分别为f1和f2)
     **/
    public static boolean createTable(String tableName, String seriesStr) {
        boolean isSuccess = false;// 判断是否创建成功！初始值为false
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            admin = connection.getAdmin();
            if (!admin.tableExists(table)) {
                System.out.println("INFO:Hbase::  " + tableName + "原数据库中表不存在！开始创建...");
                HTableDescriptor descriptor = new HTableDescriptor(table);
                Arrays.asList(seriesStr.split(","))
                        .forEach(s ->  descriptor.addFamily(new HColumnDescriptor(s.getBytes())));
                admin.createTable(descriptor);
                System.out.println("INFO:Hbase::  "+tableName + "新的" + tableName + "表创建成功！");
                isSuccess = true;
            } else {
                System.out.println("INFO:Hbase::  该表已经存在，不需要在创建！");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(admin);
        }
        return isSuccess;
    }

    /**
     * 插入数据
     * @param tableName 表名
     * @param rowkey 行健
     * @param family 列族
     * @param qualifier 列描述符
     * @param value 值
     * @throws IOException 异常信息
     */
    public static  void putDataH(String tableName, String rowkey, String family,
                                 String qualifier, Object value) throws IOException {
        Admin admin = null;
        TableName tN = TableName.valueOf(tableName);
        admin = connection.getAdmin();
        if (admin.tableExists(tN)) {
            try (Table table = connection.getTable(TableName.valueOf(tableName
                    .getBytes()))) {
                Put put = new Put(rowkey.getBytes());
                put.addColumn(family.getBytes(), qualifier.getBytes(),
                        value.toString().getBytes());
                table.put(put);

            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("插入数据的表不存在，请指定正确的tableName ! ");
        }
    }

    /**
     * 根据table查询表中的所有数据 无返回值，直接在控制台打印结果
     * */
    @SuppressWarnings("deprecation")
    public static void getValueByTable(String tableName) throws Exception {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            ResultScanner rs = table.getScanner(new Scan());
            rs.forEach(r -> {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                Arrays.stream(r.raw()).forEach(keyValue -> {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + ":" + new String(keyValue.getQualifier()) + "====值:" + new String(keyValue.getValue()));
                });
            });
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(createTable("testtable","data"));
//        System.out.println("***************插入一条数据：");
//        putDataH("learmTest","test1","f1","age","4545");
//        System.out.println("****************打印表中的数据：");
//        getValueByTable("learmTest");

//        HBaseUtils.getTestDate("learmTest");

    }
}
