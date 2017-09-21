package learm.forclass.testclass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by zhoujun on 2017/9/18.
 */
public class MainHT {

    private static Configuration conf = null;

    /**
     * 初始化配置信息
     */
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.23.128");      //设置zookeeper集群
        conf.set("hbase.zookeeper.property.clientPort", "2181"); //设置zookeeper的端口
        conf.set("hbase.rootdir", "hdfs://192.168.23.128"
                + ":9000/hbase");                                // 设置 hbase 的 master 集群地址
    }

    public static void main(String[] args){
//        creatTable("t2", new String[]{"cf1", "cf2"});
        deleteTable("t2");
    }

    public static void creatTable(String tableName, String[] family){
        try {
            /** 获取连接接口*/
            Connection connection = ConnectionFactory.createConnection(conf);

            /** 获取 Admin*/
            Admin admin = connection.getAdmin();

            /** 创建 hbase table 详细描述类，添加表的列族，版本等信息*/
            HTableDescriptor ts = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.stream(family)
                    .forEach(f -> ts.addFamily(new HColumnDescriptor(f)));
            /** 创建表 */
            admin.createTable(ts);

            if(admin.tableExists(TableName.valueOf(tableName))){
                System.out.println("create successful !");
            }
            admin.close();
        }catch(IOException ioe){
            ioe.printStackTrace();
        }
    }

    public static void deleteTable(String tableName){
        try {
            /** 获取连接接口*/
            Connection connection = ConnectionFactory.createConnection(conf);
            /** 获取 Admin*/
            Admin admin = connection.getAdmin();
            if(admin.tableExists(TableName.valueOf(tableName))){
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
                System.out.println("delete successful !");
            }
            admin.close();
        }catch(IOException ioe){
            ioe.printStackTrace();
        }
    }
}
