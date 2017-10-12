package learm.forclass.utils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class HBaseUtils {

    /**
     * 连接池对象
     */
    protected static Connection connection;
    private static final String ZK_QUORUM = "hbase.zookeeper.quorum";
    private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
    /**
     * HBase位置
     */
    private static final String HBASE_POS = "zhoujun";

    /**
     * ZooKeeper位置
     */
    private static final String ZK_POS = "zhoujun";

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
     * 创建一张表
     * @param tableName
     *            创建一个表 tableName 指定的表名　 seriesStr
     * @param seriesStr
     *            以字符串的形式指定表的列族，每个列族以逗号的形式隔开,(例如：＂f1,f2＂两个列族，分别为f1和f2)
     **/
    public static boolean createTable(String tableName, String seriesStr) {
        boolean isSuccess = false;// 判断是否创建成功！初始值为false
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            admin = connection.getAdmin();
            if (!admin.tableExists(table)) {
                System.out.println("原数据库中表不存在！开始创建...");
                HTableDescriptor descriptor = new HTableDescriptor(table);
                String[] series = seriesStr.split(",");
                for (String s : series) {
                    descriptor.addFamily(new HColumnDescriptor(s.getBytes()));
                }
                admin.createTable(descriptor);
                isSuccess = true;
            } else {
                System.out.println(" 该表已经存在，不需要在创建！");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(admin);
        }
        return isSuccess;
    }

    /**
     * 删除指定表名的表
     * @param tableName 　表名
     * @throws IOException
     * */
    public static boolean dropTable(String tableName) throws IOException {
        boolean isSuccess = false;// 判断是否创建成功！初始值为false
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            admin = connection.getAdmin();
            if (admin.tableExists(table)) {
                admin.disableTable(table);
                admin.deleteTable(table);
                isSuccess = true;
            }
        } finally {
            IOUtils.closeQuietly(admin);
        }
        return isSuccess;
    }

    /**
     * 获取一张表的实例
     * @param tableName 表名
     * @return
     */
    public static Table getTable(String tableName){
        try {
            return connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 向指定表中插入数据
     *
     * @param tableName
     *            要插入数据的表名
     * @param rowkey
     *            指定要插入数据的表的行键
     * @param family
     *            指定要插入数据的表的列族family
     * @param qualifier
     *            要插入数据的qualifier
     * @param value
     *            要插入数据的值value
     * */
    public static  void putDataH(String tableName, String rowkey, String family,
                                 String qualifier, Object value) throws IOException {
        Admin admin = null;
        TableName tN = TableName.valueOf(tableName);
        admin = connection.getAdmin();
        if (admin.tableExists(tN)){
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

}
