package learm.forclass.experiment;

import learm.forclass.utils.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class EXP02{
    /**
     * 列族 f ， 一个列族
     * 1, 编号 id
     * 2, 时间 T  作为行健
     * 3, 区域 L
     * 4, 电流 I
     * 5, 电压 V
     * 6, 功率 P
     */
    public static void main(String[] args){
//        writeData();
        writeHbase(getDataToList());
    }

    /**
     * 自动生成一分钟内的模拟数据
     */
    static void writeData(){
        // file(内存)----输入流---->【程序】----输出流---->file(内存)
        File file = new File("E:\\git\\java\\hbase\\src\\main\\resources", "electricity.txt");
        try {
            boolean newFile = file.createNewFile();// 创建文件
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 向文件写入内容(输出流)

        String[] location = {"001","002","003","004","005","006","007",
                "008","009","010","011","012","013","014","015"};
        String data = " ";
        String data2 = " ";
        String data3 = " ";
        String data4 = " ";
        String data5 = " ";
        DecimalFormat  df  = new DecimalFormat("######0.00");
        for(int j=0; j< 60; j++){
            for(int i=0; i<10000; i++){
                String id = i + "";
                if(i<10) id = "0000" + i;
                else if(i<100) id = "000" + i;
                else if(i<1000) id = "00" + i;
                else id = "0" + i;
                String time = System.currentTimeMillis() + "";
                String electricity = (int)(Math.random() * 10) + "";
                String local = location[(int)(Math.random() * 15)];
                String voltage = (218+(int)(Math.random()*6)) + "";
                String power = df.format(0.8 + ((int)(Math.random()*15))*0.01) + "";

                String electricity2 = (int)(Math.random() * 10) + "";
                String voltage2 = (218+(int)(Math.random()*6)) + "";
                String power2 = df.format(0.8 + ((int)(Math.random()*15))*0.01) + "";

                String electricity3 = (int)(Math.random() * 10) + "";
                String voltage3 = (218+(int)(Math.random()*6)) + "";
                String power3 = df.format(0.8 + ((int)(Math.random()*15))*0.01) + "";

                String electricity4 = (int)(Math.random() * 10) + "";
                String voltage4 = (218+(int)(Math.random()*6)) + "";
                String power4 = df.format(0.8 + ((int)(Math.random()*15))*0.01) + "";

                String electricity5 = (5+(int)(Math.random() * 6)) + "";
                String voltage5 = (218+(int)(Math.random()*6)) + "";
                String power5 = df.format(0.8 + ((int)(Math.random()*15))*0.01) + "";
                data = " " + id + "," + time + "," + local + ","+electricity + ","+
                        voltage + "," + power + "\n\t";
                data2 = " " + id + "," + time + "," + local + "," + electricity2 + ","+
                        voltage2 + "," + power2 + "\n\t";
                data3 = " " + id + "," + time + "," + local + "," + electricity3 + ","+
                        voltage3 + "," + power3 + "\n\t";
                data4 = " " + id + "," + time + "," + local + "," + electricity4 + ","+
                        voltage4 + "," + power4 + "\n\t";
                data5 = " " + id + "," + time + "," + local + "," + electricity5 + ","+
                        voltage5 + "," + power5 + "\n\t";
//            System.out.println("1-----  id: = " + id +
//                    "   time: = " + time + "  electricity: = " + electricity2
//                    + "  local: = " + local + "  voltage: = " + voltage2 + "  power: = "+
//                   power2);
//
//            System.out.println("2-----  id: = " + id +
//                    "   time: = " + time + "  electricity: = " + electricity3
//                    + "  local: = " + local + "  voltage: = " + voltage3 + "  power: = "+
//                    power3);
//
//            System.out.println("3-----  id: = " + id +
//                    "   time: = " + time + "  electricity: = " + electricity
//                    + "  local: = " + local + "  voltage: = " + voltage + "  power: = "+
//                    power);
//
//            System.out.println("4-----  id: = " + id +
//                    "   time: = " + time + "  electricity: = " + electricity4
//                    + "  local: = " + local + "  voltage: = " + voltage4 + "  power: = "+
//                    power4);
//
//            System.out.println("5-----  id: = " + id +
//                    "   time: = " + time + "  electricity: = " + electricity5
//                    + "  local: = " + local + "  voltage: = " + voltage5 + "  power: = "+
//                    power5);
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                byte bt[] = new byte[1024];
                byte bt2[] = new byte[1024];
                byte bt3[] = new byte[1024];
                byte bt4[] = new byte[1024];
                byte bt5[] = new byte[1024];
                bt = data.getBytes();
                bt2 = data.getBytes();
                bt3 = data.getBytes();
                bt4 = data.getBytes();
                bt5 = data.getBytes();

                RandomAccessFile randomFile = null;
                try {
                    // 打开一个随机访问文件流，按读写方式
                    randomFile = new RandomAccessFile("E:\\git\\java\\hbase\\src\\main\\resources\\electricity.txt", "rw");
                    // 文件长度，字节数
                    long fileLength = randomFile.length();
                    // 将写文件指针移到文件尾。
                    randomFile.seek(fileLength);
                    randomFile.writeBytes(data);
                    randomFile.writeBytes(data2);
                    randomFile.writeBytes(data3);
                    randomFile.writeBytes(data4);
                    randomFile.writeBytes(data5);
                } catch (IOException e) {
                    e.printStackTrace();
                } finally{
                    if(randomFile != null){
                        try {
                            randomFile.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            System.out.println("j= " + j);
        }
    }

    /**
     * 将数据生成为List<<Put>>
     */
    static List<List<Put>> getDataToList(){
        List<List<Put>> data = new ArrayList<List<Put>>();
        File file = new File("E:\\git\\java\\hbase\\src\\main\\resources\\electricity.txt");
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            int index = 0; // 标识一个List的数据
            List<Put> tempdata = new ArrayList<>();
            String tmpid = ""; // 记录每行的 id 号
            int factDataOfSeconds = 0; // 记录一个位置一秒钟的五条数据索引
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
//                System.out.println("line " + line + ": " + tempString);
                line++;
                if(index < 50000) index++;
                else{
                    data.add(tempdata); // 添加到要返回的总的数据List中
                    if(data.size() >40) break;
                    System.out.println(" 此时 data 的大小为：" + data.size());
                    tempdata = new ArrayList<Put>();
                    index =0 ;
                }
                String[] factData = tempString.split(",");
                if(factData.length == 6){
                    if(!tmpid.equals(factData[0])){
                        tmpid = factData[0];
                        factDataOfSeconds = 0;
                    }else factDataOfSeconds++;
                    /**
                     *  data =  " " + id + "," + time + "," + local + "," + electricity4 + ","+
                           voltage4 + "," + power4 + "\n\t";
                     */
                    Put put = new Put(Bytes.toBytes(factData[1] + "--" + factDataOfSeconds));
                    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("id"),
                            Bytes.toBytes(factData[0]));
                    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("L"),
                            Bytes.toBytes(factData[2]));
                    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("I"),
                            Bytes.toBytes(factData[3]));
                    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("V"),
                            Bytes.toBytes(factData[4]));
                    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("P"),
                            Bytes.toBytes(factData[5]));
                    tempdata.add(put);
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ignored) {
                    ignored.printStackTrace();
                }
            }
        }
        
        return data;
    }

    /**
     * 写入数据到 hbase 数据库中
     * @param data 要写入的数据 List 的每一项为 50000条的数据
     */
    static void writeHbase(List<List<Put>> data){
        Configuration conf = HBaseConfiguration.create();
        Connection connection;
        
        try {
            connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            System.out.println("原数据库中表不存在！开始创建...");
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf("electricity"));
            descriptor.addFamily(new HColumnDescriptor("f".getBytes()));
            admin.createTable(descriptor);

//            if (!admin.tableExists(TableName.valueOf("electricity"))) {
//                System.out.println("原数据库中表不存在！开始创建...");
//                HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf("electricity"));
//                descriptor.addFamily(new HColumnDescriptor("f".getBytes()));
//                admin.createTable(descriptor);
//            } else {
//                System.out.println(" 该表已经存在，不需要在创建！");
//            }
            admin.close();
            Table electricityTable = connection.getTable(TableName.valueOf("electricity"));
            System.out.println("开始插入数据时的时间为： " + ( new Date()));
            for(List<Put> tmpData: data ){
                electricityTable.put(tmpData);
            }
            System.out.println("结束插入数据时的时间为： " + ( new Date()));
            electricityTable.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        
        
    }


}
