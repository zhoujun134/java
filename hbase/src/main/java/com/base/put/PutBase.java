package com.base.put;

import com.utils.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by zhoujun on 2017/9/15.
 */
public class PutBase {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HBaseHelper.ZK_CLIENT_PORT_KEY,HBaseHelper.getZkPortValue());
        conf.set(HBaseHelper.ZK_QUORUM_KEY, HBaseHelper.getZkValue());
        conf.set(HBaseHelper.HBASE_KEY, HBaseHelper.getHbaseValue());
        HBaseHelper helper = HBaseHelper.getHelper(conf);
//        helper.createTable("basehbase", "f1", "f2", "f3");

        Table table = helper.getConnection().getTable(TableName.valueOf("basehbase"));
        Put put = new Put("row2".getBytes());
        put.addColumn("f1".getBytes(), "age".getBytes(), "21".getBytes());
        Put put2 = new Put("row2".getBytes());
        put2.addColumn("f1".getBytes(), "name".getBytes(), "zhangshan".getBytes());
        //批量插入两条数据
        table.put(Arrays.<Put>asList(put, put2));

        //插入一条数据
        Put put3 = new Put("row3".getBytes());
        put.addColumn("f1".getBytes(), "name".getBytes(), "value2".getBytes());
        table.put(put3);

        //扫描打印
        Scan scan = new Scan();
               scan.addFamily("f1".getBytes());

        table.getScanner(scan)
                .forEach(result ->{
                        result.getMap()
                                .forEach((k,v) ->{
                                       log_Info(new String(k));
                                       v.forEach((k2,v2) ->{
                                           log_Info(new String(k2));
                                           v2.forEach((k3,v3) -> log_Info(new String(k3.toString()) + " value：" + new String(v3)));
                                       });
                        });
                });
    }

    /**
     * 日志信息类
     */
    public static void log_Info(Object msg){
        System.out.println("信息：" + msg);
    }

}
