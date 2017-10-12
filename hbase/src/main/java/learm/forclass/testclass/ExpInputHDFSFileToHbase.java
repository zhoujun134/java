package learm.forclass.testclass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
/**
 * Created by zhoujun on 2017/9/28.
 * 实验 三
 */
public class ExpInputHDFSFileToHbase {
    private static Configuration hconf = HBaseConfiguration.create();

    public static void main(String[] args){
        putDataToHbase(getDate());
    }

    /**
     * 插入数据
     * @param list 数据列表
     */
    private static void putDataToHbase(ArrayList<String> list){
        Table table = null;
        try {
            Connection connection = ConnectionFactory.createConnection(hconf);
            Admin admin = connection.getAdmin();
            //创建表
            HTableDescriptor ts = new HTableDescriptor(TableName.valueOf("classtable"));
            ts.addFamily(new HColumnDescriptor("f"));
            if(!admin.tableExists(TableName.valueOf("classtable"))) admin.createTable(ts);
            //获取表的实例
            table = connection.getTable(TableName.valueOf("classtable"));
            ArrayList<Put> list1 = new ArrayList<Put>();
            for(String s: list){
                String[] data = s.split(" ");
                String row = "";
                if(convert(data) != 0 ) row = convert(data)+"";
                else continue;
                Put put = new Put(Bytes.toBytes(row));
                put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("level"),Bytes.toBytes(data[2]));

                Put put2 = new Put(Bytes.toBytes(row));
                put2.addColumn(Bytes.toBytes("f"), Bytes.toBytes("class"),Bytes.toBytes(data[5]));

                list1.add(put);
                list1.add(put2);
            }
            if(list1.size()>0) table.put(list1);

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                assert table != null;
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取数据列表
     * @return
     */
    private static ArrayList<String> getDate(){
        ArrayList<String> result = new ArrayList<>();
        try {
            FileReader fr=new FileReader("/opt/hbase/logs/hbase-zhoujun-master-zhoujun.log");
            BufferedReader br=new BufferedReader(fr);
            String line = "";
            while ((line=br.readLine())!=null) {
                String[] data = line.split(" ");
                if(data.length >= 4 && ( data[2].equals("INFO") || data[2].equals("WARN") || data[2].equals("ERROR")|| data[2].equals("FATAL") )
                        && data[0].length()<= 10 ){
                    System.out.println(convert(data));
                    result.add(line);
                }
            }
            br.close();
            fr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 转换时间格式
     * @param strs 字符串数组
     * @return
     */
    private static long convert(String[] strs ){
        //Date或者String转化为时间戳
        SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time= strs[0] + " " + strs[1];
        System.out.println("转换的时间是： " + time);
        Date date = null;
        try {
            date = format.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if(date != null) {
            long timestamp = date.getTime();
            System.out.println("Format To times:" + timestamp);
            return Long.MAX_VALUE - timestamp;
        }return 0;
    }

}
