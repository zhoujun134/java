package com.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by zhoujun on 2017/9/15.
 */
public class HBaseHelper implements Closeable {
    /**
     * zookeeper 地址 key
     */
    public final static String ZK_QUORUM_KEY = "hbase.zookeeper.quorum";

    /***
     * zookeeper 集群端口地址 key
     */
    public final static String ZK_CLIENT_PORT_KEY = "hbase.zookeeper.property.clientPort";

    /**
     * HBase 位置 key
     */
    public final static String HBASE_KEY = "hbase.rootdir";

    /**
     * HBase 位置 value
     * 默认值是：192.168.27.132:9000 下的 hbase 数据库
     */
    private static String HBASE_VALUE = "hdfs://192.168.1.104:9000/hbase";

    /**
     * ZooKeeper 位置 value
     * 默认值是：192.168.27.132
     */
    private static String ZK_VALUE = "192.168.1.104";

    /**
     * zookeeper 服务端口 value
     * 默认值是：2181
     */
    private static String ZK_PORT_VALUE = "2181";

    /**
     *  配置类
     */
    private Configuration configuration = null;

    /**
     * 连接类
     */
    private Connection connection = null;

    /**
     * 管理类
     */
    private Admin admin = null;

    /**
     * 构造函数
     * @param configuration 配置信息
     * @throws IOException
     */
    protected HBaseHelper(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.connection = ConnectionFactory.createConnection(configuration);
        this.admin = connection.getAdmin();
    }

    /**
     * 获取HBaseHelper 实例
     * @param configuration 配置信息
     * @return
     * @throws IOException
     */
    public static HBaseHelper getHelper(Configuration configuration) throws IOException {
        return new HBaseHelper(configuration);
    }

    /**
     * 获取HBaseHelper 实例
     * @return
     * @throws IOException
     */
    public static HBaseHelper getHelper() throws IOException{
        Configuration conf = HBaseConfiguration.create();
        conf.set(HBaseHelper.ZK_CLIENT_PORT_KEY,HBaseHelper.getZkPortValue());
        conf.set(HBaseHelper.ZK_QUORUM_KEY, HBaseHelper.getZkValue());
        conf.set(HBaseHelper.HBASE_KEY, HBaseHelper.getHbaseValue());
        return new HBaseHelper(conf);
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }

    /**
     * 获取连接对象
     * @return
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * 获取配置信息实例对象
     * @return
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * 创建命名空间
     * @param namespace 命名空间
     */
    public void createNamespace(String namespace) {
        try {
            NamespaceDescriptor nd = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(nd);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    /**
     * 删除命名空间
     * @param namespace 命名空间
     * @param force  是否该命名空间下的删除所有表
     */
    public void dropNamespace(String namespace, boolean force) {
        try {
            if(force) {
                TableName[] tableNames = admin.listTableNamesByNamespace(namespace);
                for (TableName name : tableNames) {
                    admin.disableTable(name);
                    admin.deleteTable(name);
                }
            }
        } catch (Exception e) {
            // ignore
        }
        try {
            admin.deleteNamespace(namespace);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    /**
     * 检查是否存在制定的表名
     * @param table 表名
     * @return
     * @throws IOException
     */
    public boolean existsTable(String table)
            throws IOException {
        return existsTable(TableName.valueOf(table));
    }

    /**
     * 检查是否存在制定的表名
     * @param table 表名
     * @return
     * @throws IOException
     */
    public boolean existsTable(TableName table)
            throws IOException {
        return admin.tableExists(table);
    }

    /**
     * 创建一张表，默认的最大版本为：1， 不使用分区
     * @param table 表名
     * @param colfams 列祖成员
     * @throws IOException
     */
    public void createTable(String table, String... colfams)
            throws IOException {
        createTable(TableName.valueOf(table), 1, null, colfams);
    }

    /**
     * 创建一张表，默认的最大版本为：1， 不使用分区
     * @param table 表名
     * @param colfams 列族元素
     * @throws IOException
     */
    public void createTable(TableName table, String... colfams)
            throws IOException {
        createTable(table, 1, null, colfams);
    }

    /**
     * 指定最大版本创建一张表， 不使用分区
     * @param table 表名
     * @param maxVersions 最大版本值
     * @param colfams 列族元素
     * @throws IOException
     */
    public void createTable(String table, int maxVersions, String... colfams)
            throws IOException {
        createTable(TableName.valueOf(table), maxVersions, null, colfams);
    }

    /**
     * 指定最大版本的创建一张表， 不使用分区
     * @param table 表名
     * @param maxVersions 最大版本值
     * @param colfams 列族元素
     * @throws IOException
     */
    public void createTable(TableName table, int maxVersions, String... colfams)
            throws IOException {
        createTable(table, maxVersions, null, colfams);
    }

    /**
     * 创建一张表，最大版本数使 1， 使用分区
     * @param table 表名
     * @param splitKeys 分区的数组
     * @param colfams 列族元素
     * @throws IOException
     */
    public void createTable(String table, byte[][] splitKeys, String... colfams)
            throws IOException {
        createTable(TableName.valueOf(table), 1, splitKeys, colfams);
    }

    /**
     * 指定表名，版本，分区列表，列族创建一张表
     * @param table 表名
     * @param maxVersions 最大版本值
     * @param splitKeys 分区数
     * @param colfams 列族元素
     * @throws IOException
     */
    public void createTable(TableName table, int maxVersions, byte[][] splitKeys,
                            String... colfams)
            throws IOException {
        HTableDescriptor desc = new HTableDescriptor(table);
        Arrays.stream(colfams)
                .forEach(s -> {
                    HColumnDescriptor coldef = new HColumnDescriptor(s);
                    coldef.setMaxVersions(maxVersions);
                    desc.addFamily(coldef);
                });
        if (splitKeys != null) admin.createTable(desc, splitKeys);
        else  admin.createTable(desc);
    }

    /**
     * 指定对应表失效
     * @param table 表名
     * @throws IOException
     */
    public void disableTable(String table) throws IOException {
        disableTable(TableName.valueOf(table));
    }

    /**
     * 指定对应表失效
     * @param table 表名
     * @throws IOException
     */
    public void disableTable(TableName table) throws IOException {
        admin.disableTable(table);
    }

    /**
     * 删除对应表
     * @param table 表名
     * @throws IOException
     */
    public void dropTable(String table) throws IOException {
        dropTable(TableName.valueOf(table));
    }

    /**
     * 删除对应表
     * @param table 表名
     * @throws IOException
     */
    public void dropTable(TableName table) throws IOException {
        if (existsTable(table)) {
            if (admin.isTableEnabled(table)) disableTable(table);
            admin.deleteTable(table);
        }
    }


    /**
     * 填充表的数据
     * @param table
     * @param startRow
     * @param endRow
     * @param numCols
     * @param colfams
     * @throws IOException
     */
    public void fillTable(String table, int startRow, int endRow, int numCols,
                          String... colfams)
            throws IOException {
        fillTable(TableName.valueOf(table), startRow, endRow, numCols, colfams);
    }

    public void fillTable(TableName table, int startRow, int endRow, int numCols,
                          String... colfams)
            throws IOException {
        fillTable(table, startRow, endRow, numCols, -1, false, colfams);
    }

    public void fillTable(String table, int startRow, int endRow, int numCols,
                          boolean setTimestamp, String... colfams)
            throws IOException {
        fillTable(TableName.valueOf(table), startRow, endRow, numCols, -1,
                setTimestamp, colfams);
    }

    public void fillTable(TableName table, int startRow, int endRow, int numCols,
                          boolean setTimestamp, String... colfams)
            throws IOException {
        fillTable(table, startRow, endRow, numCols, -1, setTimestamp, colfams);
    }

    public void fillTable(String table, int startRow, int endRow, int numCols,
                          int pad, boolean setTimestamp, String... colfams)
            throws IOException {
        fillTable(TableName.valueOf(table), startRow, endRow, numCols, pad,
                setTimestamp, false, colfams);
    }

    public void fillTable(TableName table, int startRow, int endRow, int numCols,
                          int pad, boolean setTimestamp, String... colfams)
            throws IOException {
        fillTable(table, startRow, endRow, numCols, pad, setTimestamp, false,
                colfams);
    }

    public void fillTable(String table, int startRow, int endRow, int numCols,
                          int pad, boolean setTimestamp, boolean random,
                          String... colfams)
            throws IOException {
        fillTable(TableName.valueOf(table), startRow, endRow, numCols, pad,
                setTimestamp, random, colfams);
    }

    public void fillTable(TableName table, int startRow, int endRow, int numCols,
                          int pad, boolean setTimestamp, boolean random,
                          String... colfams)
            throws IOException {
        Table tbl = connection.getTable(table);
        Random rnd = new Random();
        for (int row = startRow; row <= endRow; row++) {
            for (int col = 1; col <= numCols; col++) {
                Put put = new Put(Bytes.toBytes("row-" + padNum(row, pad)));
                for (String cf : colfams) {
                    String colName = "col-" + padNum(col, pad);
                    String val = "val-" + (random ?
                            Integer.toString(rnd.nextInt(numCols)) :
                            padNum(row, pad) + "." + padNum(col, pad));
                    if (setTimestamp) {
                        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), col,
                                Bytes.toBytes(val));
                    } else {
                        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName),
                                Bytes.toBytes(val));
                    }
                }
                tbl.put(put);
            }
        }
        tbl.close();
    }

    public void fillTableRandom(String table,
                                int minRow, int maxRow, int padRow,
                                int minCol, int maxCol, int padCol,
                                int minVal, int maxVal, int padVal,
                                boolean setTimestamp, String... colfams)
            throws IOException {
        fillTableRandom(TableName.valueOf(table), minRow, maxRow, padRow, minCol,
                maxCol, padCol, minVal, maxVal, padVal, setTimestamp, colfams);
    }

    public void fillTableRandom(TableName table,
                                int minRow, int maxRow, int padRow,
                                int minCol, int maxCol, int padCol,
                                int minVal, int maxVal, int padVal,
                                boolean setTimestamp, String... colfams)
            throws IOException {
        Table tbl = connection.getTable(table);
        Random rnd = new Random();
        int maxRows = minRow + rnd.nextInt(maxRow - minRow);
        for (int row = 0; row < maxRows; row++) {
            int maxCols = minCol + rnd.nextInt(maxCol - minCol);
            for (int col = 0; col < maxCols; col++) {
                int rowNum = rnd.nextInt(maxRow - minRow + 1);
                Put put = new Put(Bytes.toBytes("row-" + padNum(rowNum, padRow)));
                for (String cf : colfams) {
                    int colNum = rnd.nextInt(maxCol - minCol + 1);
                    String colName = "col-" + padNum(colNum, padCol);
                    int valNum = rnd.nextInt(maxVal - minVal + 1);
                    String val = "val-" +  padNum(valNum, padCol);
                    if (setTimestamp) {
                        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), col,
                                Bytes.toBytes(val));
                    } else {
                        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName),
                                Bytes.toBytes(val));
                    }
                }
                tbl.put(put);
            }
        }
        tbl.close();
    }

    public String padNum(int num, int pad) {
        String res = Integer.toString(num);
        if (pad > 0) {
            while (res.length() < pad) {
                res = "0" + res;
            }
        }
        return res;
    }

    /**
     * 插入一条数据
     * @param table 表名
     * @param row 行健
     * @param fam 列族
     * @param qual 列限定符
     * @param val 值
     * @throws IOException
     */
    public void put(String table, String row, String fam, String qual,
                    String val) throws IOException {
        put(TableName.valueOf(table), row, fam, qual, val);
    }

    /**
     * 插入一条数据
     * @param table 表名
     * @param row 行健
     * @param fam 列族
     * @param qual 列限定符
     * @param val 值
     * @throws IOException
     */
    public void put(TableName table, String row, String fam, String qual,
                    String val) throws IOException {
        Table tbl = connection.getTable(table);
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), Bytes.toBytes(val));
        tbl.put(put);
        tbl.close();
    }

    /**
     * 插入一条数据，指定时间戳
     * @param table 表名
     * @param row 行健
     * @param fam 列族
     * @param qual 列限定副
     * @param ts 时间戳
     * @param val 值
     * @throws IOException
     */
    public void put(String table, String row, String fam, String qual, long ts,
                    String val) throws IOException {
        put(TableName.valueOf(table), row, fam, qual, ts, val);
    }

    /**
     * 插入一条数据，指定时间戳
     * @param table 表名
     * @param row 行健
     * @param fam 列族
     * @param qual 列限定副
     * @param ts 时间戳
     * @param val 值
     * @throws IOException
     */
    public void put(TableName table, String row, String fam, String qual, long ts,
                    String val) throws IOException {
        Table tbl = connection.getTable(table);
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), ts,
                Bytes.toBytes(val));
        tbl.put(put);
        tbl.close();
    }

    /**
     * 插入多条条数据
     * @param table 表名
     * @param rows 行健
     * @param fams 列族
     * @param quals 列限定副
     * @param ts 时间戳
     * @param vals 值
     * @throws IOException
     */
    public void put(String table, String[] rows, String[] fams, String[] quals,
                    long[] ts, String[] vals) throws IOException {
        put(TableName.valueOf(table), rows, fams, quals, ts, vals);
    }

    /**
     * 插入多条条数据
     * @param table 表名
     * @param rows 行健
     * @param fams 列族
     * @param quals 列限定副
     * @param ts 时间戳
     * @param vals 值
     * @throws IOException
     */
    public void put(TableName table, String[] rows, String[] fams, String[] quals,
                    long[] ts, String[] vals) throws IOException {
        Table tbl = connection.getTable(table);
        Arrays.stream(rows).forEach(row ->{
            Put put = new Put(Bytes.toBytes(row));
                    Arrays.stream(quals).forEach(fam ->{
                        final int[] v = {0};
                        Arrays.stream(quals).forEach(qual ->{
                            String val = vals[v[0] < vals.length ? v[0] : vals.length - 1];
                            long t = ts[v[0] < ts.length ? v[0] : ts.length - 1];
                            System.out.println("Adding: " + row + " " + fam + " " + qual +
                                    " " + t + " " + val);
                            put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), t,
                                    Bytes.toBytes(val));
                            v[0]++;
                        });
                    });
            try {
                tbl.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        tbl.close();

    }

    /**
     * 删除对应的数据
     * @param table 表名
     * @param rows 行健
     * @param fams 列族
     * @param quals 列限定符
     * @throws IOException
     */
    public void dump(String table, String[] rows, String[] fams, String[] quals)
            throws IOException {
        dump(TableName.valueOf(table), rows, fams, quals);
    }

    /**
     * 获取对应的数据
     * @param table 表名
     * @param rows 行健
     * @param fams 列族
     * @param quals 列限定符
     * @throws IOException
     */
    public void dump(TableName table, String[] rows, String[] fams, String[] quals)
            throws IOException {
        Table tbl = connection.getTable(table);
        List<Get> gets = new ArrayList<Get>();
        Arrays.stream(rows).forEach(row -> {
            Get get = new Get(Bytes.toBytes(row));
            get.setMaxVersions();
            if (fams != null) {
                Arrays.stream(fams).forEach(fam ->{
                    Arrays.stream(quals).forEach(qual -> get.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual)));
                });
            }
            gets.add(get);
        });
        Result[] results = tbl.get(gets);
        Arrays.stream(results).forEach(result -> {
            Arrays.stream(result.rawCells()).forEach(cell ->{
                System.out.println("Cell: " + cell +
                        ", Value: " + Bytes.toString(cell.getValueArray(),
                        cell.getValueOffset(), cell.getValueLength()));
            });
        });
        tbl.close();
    }

    public void dump(String table) throws IOException {
        dump(TableName.valueOf(table));
    }

    public void dump(TableName table) throws IOException {
        try (
                Table t = connection.getTable(table);
                ResultScanner scanner = t.getScanner(new Scan())
        ){
           scanner.forEach(result -> dumpResult(result));
        }
    }

    public void dumpResult(Result result) {
        for (Cell cell : result.rawCells()) {
            System.out.println("Cell: " + cell +
                    ", Value: " + Bytes.toString(cell.getValueArray(),
                    cell.getValueOffset(), cell.getValueLength()));
        }
    }


    public static String getHbaseValue() {
        return HBASE_VALUE;
    }

    public static void setHbaseValue(String hbaseValue) {
        HBASE_VALUE = hbaseValue;
    }

    public static String getZkValue() {
        return ZK_VALUE;
    }

    public static void setZkValue(String zkValue) {
        ZK_VALUE = zkValue;
    }

    public static String getZkPortValue() {
        return ZK_PORT_VALUE;
    }

    public static void setZkPortValue(String zkPortValue) {
        ZK_PORT_VALUE = zkPortValue;
    }
}
