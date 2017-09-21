package learm.forclass.testclass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by zhoujun on 2017/9/21.
 * 实验 二
 */
public class MainHT {

    /**
     * 连接配置信息
     */
    private static Configuration conf = null;

    /**
     * 情态代码块，初始化配置信息，static 中的代码会在 main 方法调用之前执行
     */
    static {
        conf = HBaseConfiguration.create();// 初始化配置信息
//        conf.set("hbase.zookeeper.quorum","192.168.23.128");      //设置zookeeper集群
//        conf.set("hbase.zookeeper.property.clientPort", "2181"); //设置zookeeper的端口
//        conf.set("hbase.rootdir", "hdfs://192.168.23.128"
//                + ":9000/hbase");                                // 设置 hbase 的 master 集群地址
    }


    public static void main(String[] args) throws InterruptedException {
        log("**************  主方法调试 ******************");
        // 列出数据库中所有的表
        listTables();
        // 创建一个表，包含五个分区，两个列族
        createTableThroughRegion(TableName.valueOf("testRegion"), new String[]{"cf1","cf2"},
                new String[]{"10|", "20|", "30|", "40|", "50|"});
        // 删除表中的一个列族
       deleteAFamily(TableName.valueOf("testRegion"), "cf1");
        // 列出数据库中所有的表
        log("删除一个列族后的表结构：");
        listTables();

        // 创建表
        log("创建几个测试使用的表");
        creatTable("t1", new String[]{"tc1"});
        creatTable("t2", new String[]{"tc1", "tc2"});
        creatTable("test3", new String[]{"tc1"});
        creatTable("test4", new String[]{"tc1", "tc2"});

        // 列出数据库中所有的表
        log("创建之后的数据库中的表为：");
        listTables();

        log("******************************************");
        log("**************  删除一张表 t2  ************");
        // 列出数据库中所有的表
        log("删除之前数据库中的表有：");
        listTables();
        // 删除 t2 这张表
        deleteTable("t1");

        log("删除之后数据库中的表有：");
        listTables();    // 列出数据库中所有的表
        Thread.sleep(1000);
        log("******************************************");
        log("**************  删除所有表  ****************");
        // 列出数据库中所有的表
        log("全部删除之前数据库中的表有：");
        listTables();
        // 全部删除
        deleteAllTable();
        log("全部删除之后数据库中的表有：");
        listTables();
        Thread.sleep(1000);
        log("******************************************");
    }

    /**
     * 根据指定的表名和列族列表，创建一张表
     * @param tableName 创建的表名
     * @param family 创建的列族数组
     */
    public static void creatTable(String tableName, String[] family){
        Admin admin = null;
        try {
            /** 获取连接接口*/
            Connection connection = ConnectionFactory.createConnection(conf);
            /** 获取 Admin*/
            admin = connection.getAdmin();
            /** 创建 hbase table 详细描述类，添加表的列族，版本等信息*/
            HTableDescriptor ts = new HTableDescriptor(TableName.valueOf(tableName));
            for(String f : family) ts.addFamily(new HColumnDescriptor(f));

            /** 创建表 */
            admin.createTable(ts);

            if(admin.tableExists(TableName.valueOf(tableName))){
                log("创建表：" + tableName + " 成功 ！");
                log("列族伪： " );
                for(String f : family) log("---" + f);
            }
        }catch(IOException ioe){
            log("******************创建表失败");
            ioe.printStackTrace();
        }finally {
            try{
                assert admin != null;
                admin.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    /**
     * 指定表名，删除一张表
     * @param tableName 删除表的表名
     */
    public static void deleteTable(String tableName){
        Admin admin2 = null;
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            admin2 = connection.getAdmin();
            if(admin2.tableExists(TableName.valueOf(tableName))){
                admin2.disableTable(TableName.valueOf(tableName));
                if(!admin2.isTableEnabled(TableName.valueOf(tableName))){
                    admin2.deleteTable(TableName.valueOf(tableName));
                    log("删除表 " + tableName + "  成功！");
                }else{
                    log("表不存在 !");
                }
            }
        }catch(IOException ioe){
            ioe.printStackTrace();
        }finally {
        try{
            assert admin2 != null;
            admin2.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    }

    /**
     * 删除数据库中的所有表
     */
    public static void deleteAllTable(){
        Admin admin3 = null;
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            admin3 = connection.getAdmin();

            TableName[] tableNamess = admin3.listTableNames();
            log("开始删除：");
            for(TableName tableName: tableNamess){
                if(admin3.tableExists(tableName)){
                    admin3.disableTable(tableName);
                    if(!admin3.isTableEnabled(tableName)){
                        admin3.deleteTable(tableName);
                        log("删除表 " + tableName + "  成功！");
                    }else{
                        log("*********** 表不可使用 !");
                    }
                }else log("表不存在 ！");
            }
        }catch(IOException ioe){
            ioe.printStackTrace();
        }finally {
            try{
                assert admin3 != null;
                admin3.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    /**
     * 列出所有的表名，列族
     */
    public static void listTables(){
        Admin admin3 = null;
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            admin3 = connection.getAdmin();
            TableName[] tableNamess = admin3.listTableNames();
            log("现在数据库有的表：");
            log("************************");
            for(TableName tableName: tableNamess){
                HTableDescriptor hTableDescriptor = admin3.getTableDescriptor(tableName);
                log("*********");
                log("***表名: " + tableName);
                log("***列族：");
                for(HColumnDescriptor hcp : hTableDescriptor.getFamilies()){
                    log("  " + hcp.getNameAsString());
                }
                log("*********");
            }
            log("************************");
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            try {
                assert admin3 != null;
                admin3.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建有 region 分区的表
     * @param tableName 表名
     * @param family 列族数组
     * @param regions 分区的数组，exp:  new String[]{"10|", "20|", "30|", "40|", "50|"}
     */
    public static void createTableThroughRegion(TableName tableName, String[] family, String[] regions){
      Admin admin = null;
      try {
          Connection connection = ConnectionFactory.createConnection(conf);
          admin = connection.getAdmin();
          HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
          for(String f: family){
              hTableDescriptor.addFamily(new HColumnDescriptor(f));
          }

          byte[][] splitKeys = new byte[regions.length][];
          /**
           * 根据传入的 region 数组直接创建
           */

          for (int i = 0; i < regions.length; i++) {
              splitKeys[i] = Bytes.toBytes(regions[i]);
          }

//          /**
//           * 给指定的 region 分区，根据其 hashCode 升序排序
//           */
//          TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
//          for (int i = 0; i < regions.length; i++) rows.add(Bytes.toBytes(regions[i]));
//          Iterator<byte[]> rowKeyIter = rows.iterator();
//          int i = 0;
//          while (rowKeyIter.hasNext()) {
//              byte[] tempRow = rowKeyIter.next();
//              rowKeyIter.remove();
//              splitKeys[i] = tempRow;
//              i++;
//          }
          //创建
          admin.createTable(hTableDescriptor, splitKeys);
      }catch (Exception e){
          e.printStackTrace();
      }finally {
          try{
              assert admin != null;
              admin.close();
          }catch (Exception e){
              e.printStackTrace();
          }
      }
    }

    /**
     * 删除指定表的一个列族
     * @param tableName 表名
     * @param deleteFamily 要删除列族的列族名
     */
    public static void deleteAFamily(TableName tableName, String deleteFamily){
        Admin admin = null;
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            if(!admin.tableExists(tableName)) throw new Exception("所要删除的表不存在");
            if(admin.isTableEnabled(tableName)){
                admin.disableTable(tableName);
                log("disable table");
                admin.deleteColumn(tableName, Bytes.toBytes(deleteFamily));
                admin.enableTable(tableName);
                log("enable table");
            }else {
                log("delete deleteFamily !");
                admin.deleteColumn(tableName, Bytes.toBytes(deleteFamily));
                admin.enableTable(tableName);
                log("enable table !");
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            try {
                assert admin != null;
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 内部工具类，打印日志信息
     * @param msg 日志信息
     */
    private static void log(Object msg){
        System.out.println("info： " + msg);
    }
}
