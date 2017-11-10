package learm.forclass.testclass;

import learm.forclass.utils.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

/**
 * Created by zhoujun on 17-10-18.
 */
public class InputBigDataTest {

    private static Configuration conf = HBaseConfiguration.create();

    private static Connection connection = null;
    static {
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException {


//        HBaseUtils.dropTable("inputTable");
        System.out.println("info: 开始时间：  " + new Date());
        inputData();
        System.out.println("info: 结束时间：  " + new Date());

//        HBaseUtils.createTable("inputTable","f");
    }


    public static void inputData() throws IOException {

        Table table = connection.getTable(TableName.valueOf("inputTable"));
        ArrayList<Put> list = new ArrayList<>();
        try {

            FileReader fr=new FileReader("/home/zhoujun/下载/hly-temp-10pctl.txt");
            BufferedReader br=new BufferedReader(fr);
            String line = "";
            while ((line=br.readLine())!=null) {
                String[] data = line.split(" ");
//                System.out.println("info: length--"+ line.length() + "  data: " + line);
                Put put = new Put(Bytes.toBytes(data[0] + "--" + data[1]));
                put.addColumn(Bytes.toBytes("f"), Bytes.toBytes(data[2]), Bytes.toBytes(line));
                list.add(put);
            }
            br.close();
            fr.close();
            table.put(list);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
