package com.classstudy;

import com.classstudy.classtest.Trader;
import com.classstudy.classtest.Transaction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class TraderTest {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("trader");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Trader raoul = new Trader("Raoul", "Cambridge");
        Trader raou2 = new Trader("Raou2", "Cambridge");
        Trader mario = new Trader("Mario","Milan");
        Trader mario2 = new Trader("Mario2","Milan");
        Trader alan = new Trader("Alan","Cambridge");
        Trader brian = new Trader("Brian","Cambridge");

        List<Transaction> transactions = Arrays.asList(
                new Transaction(brian, 2011, 300),
                new Transaction(raoul, 2012, 1000),
                new Transaction(raou2, 2011, 400),
                new Transaction(mario, 2012, 710),
                new Transaction(mario2, 2012, 700),
                new Transaction(alan, 2012, 950)
        );

        /*
        * //1.找出2011年发生的所有交易，并按交易额排序(从低到高)
        * //2.交易员都在哪些不同的城市工作过？
        * //3.查找所有来自剑桥的交易员，并按姓名排序
        * //4.返回所有交易员的姓名字符串，按字母顺序排序
        * //5.有没有交易员是在米兰工作的？
        * //6.打印生活在剑桥的交易员的所有交易额
        * //7.所有交易中，最高的交易额是多少
        * //8.找到交易额最小的交易
        * */
        JavaRDD<Transaction> rdd = sc.parallelize(transactions);

        JavaPairRDD<Transaction, Integer> pairRDD =
                rdd.mapToPair(transaction ->
                                new Tuple2<Transaction, Integer>(transaction, 1));

        // 1,找出2011年发生的所有交易，并按交易额排序(从低到高)
        System.out.println("****************************");
        System.out.println("1,找出2011年发生的所有交易，并按交易额排序(从低到高)");
           pairRDD.filter(trans -> trans._1().getYear() == 2011)
                   .sortByKey(new Sort1())
                   .map(pairTrader -> pairTrader._1())
                   .foreach(s -> System.out.println(s));

        //2.交易员都在哪些不同的城市工作过？
        System.out.println("****************************");
        System.out.println("2,交易员都在哪些不同的城市工作过？");
        rdd.map(transaction -> transaction.getTrader().getCity())
                .distinct()
                .foreach(s -> System.out.println(s));


        //3.查找所有来自剑桥的交易员，并按姓名排序
        System.out.println("****************************");
        System.out.println("3,查找所有来自剑桥的交易员，并按姓名排序？");
         pairRDD.filter(trader ->
                      trader._1().getTrader().getCity().equals("Cambridge"))
                 .distinct()
                 .sortByKey(new SortByName())
                 .map(pairTrader -> pairTrader._1().getTrader())
                 .foreach(s -> System.out.println(s));

         //4.返回所有交易员的姓名字符串，按字母顺序排序
        System.out.println("****************************");
        System.out.println("4,返回所有交易员的姓名字符串，按字母顺序排序");
        rdd.map(transaction -> transaction.getTrader().getName())
                .distinct()
                .mapToPair(s -> new Tuple2<>(s,1))
                .sortByKey(new SortByDictionary())
                .map(pair -> pair._1())
                .foreach(s -> System.out.println(s));

        //5.有没有交易员是在米兰工作的？
        System.out.println("****************************");
        System.out.println("5.有没有交易员是在米兰工作的？");
        rdd.filter(transaction -> transaction.getTrader().getCity().equals("Milan"))
                .distinct()
                .foreach(s -> System.out.println(s));

        //6.打印生活在剑桥的交易员的所有交易额
        System.out.println("****************************");
        System.out.println("6.打印生活在剑桥的交易员的所有交易额");
        rdd.filter(transaction ->
                   transaction.getTrader().getCity().equals("Cambridge"))
                .distinct()
                .foreach(s -> System.out.println(s));
        //7.所有交易中，最高的交易额是多少

        System.out.println("****************************");
        System.out.println("7.所有交易中，最高的交易额是多少   ");
        int max = rdd.collect()
                .stream()
                .map(Transaction::getValue)
                .reduce(0, Integer::max);

        System.out.println(max);
        // 8.找到交易额最小的交易
        System.out.println("****************************");
        System.out.println("8.找到交易额最小的交易 ");
        List<Integer> min = rdd.mapToPair(
                 pair -> new Tuple2<Integer, Integer>(pair.getValue(),1))
                .sortByKey(new SortValue())
                .map(pair -> pair._1())
                .take(1);
        System.out.println(min.get(0));
    }
}
class Sort1 implements Comparator<Transaction>, Serializable{
    @Override
    public int compare(Transaction o1, Transaction o2) {
        return o1.getValue() - o2.getValue();
    }
}

class SortByName implements Comparator<Transaction>, Serializable{
    @Override
    public int compare(Transaction o1, Transaction o2) {
        return o1.getTrader().getName().compareTo(o2.getTrader().getName());
    }
}

class SortByDictionary implements Comparator<String>, Serializable{
    @Override
    public int compare(String o1, String o2) {
        return -(o2.hashCode() - o1.hashCode());
    }
}

class SortValue implements Comparator<Integer>, Serializable{
    @Override
    public int compare(Integer o1, Integer o2) {
        return o1 - o2;
    }
}