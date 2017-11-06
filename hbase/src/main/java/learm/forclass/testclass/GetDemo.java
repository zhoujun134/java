package learm.forclass.testclass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigInteger;

public class GetDemo {
    public static void main(String[] args){
//        Configuration conf = HBaseConfiguration.create();
//        Table table = null;
//        try {
//            Connection connection = ConnectionFactory.createConnection(conf);
//            table = connection.getTable(TableName.valueOf("a3"));
//            Get get = new Get(Bytes.toBytes("r1"));
//            Result result = table.get(get);
//            byte[] value = result.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("name"));
//            System.out.println(" r1: cf1:namme:    value = " +Bytes.toString(value));
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }finally {
//            try {
//                assert table != null;
//                table.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        BigInteger x = new BigInteger(Bytes.toBytes(16));
//        byte[] b = String.format("%16", x).getBytes();
//        System.out.println(" b: " + Bytes.toString(b));

        byte[][] spl = getHexSplits("1","100",10);
        System.out.println(spl);

    }

    public static byte[][] getHexSplits(String startKey, String endKey, int numRegions){
        byte[][] splits = new byte[numRegions][];
        long low = Long.parseLong(startKey);
        long end = Long.parseLong(endKey);
        long regionIncr = end - low ;
        low = low + regionIncr;
        for(int i=0; i < numRegions; i++){
            long key = low + regionIncr*i;
            System.out.println("key: " + key);
            splits[i] = Bytes.toBytes(key);
        }
        return splits;
    }

}
