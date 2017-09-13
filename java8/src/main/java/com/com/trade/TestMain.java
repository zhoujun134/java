package com.com.trade;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public class TestMain {
	 public static void main(String[] args){
	        Trader raoul = new Trader("Raoul", "Cambridge");
	        Trader mario = new Trader("Mario","Milan");
	        Trader alan = new Trader("Alan","Cambridge");
	        Trader brian = new Trader("Brian","Cambridge");

	        List<Transaction> transactions = Arrays.asList(
	                new Transaction(brian, 2011, 300),
	                new Transaction(raoul, 2012, 1000),
	                new Transaction(raoul, 2011, 400),
	                new Transaction(mario, 2012, 710),
	                new Transaction(mario, 2012, 700),
	                new Transaction(alan, 2012, 950)
	        );


	        //找出 2011 年发生的所有交易，并按交易额排序
	        //方式一
//	        List<Transaction> tr2011 = transactions.stream()
//	                .filter(transaction -> transaction.getYear() == 2011)
//	                .sorted(comparing(Transaction::getValue))
//	                .collect(toList());
	        //方式二
	        List<Transaction> tr2011 = transactions.stream()
	        		.filter(trans -> trans.getYear() == 2011)
	        		.sorted((t1, t2) -> t1.getValue()>t2.getValue()? 1 : -1)
	        		.collect(Collectors.toList());
	        System.out.println(tr2011);

	        // 问题2：交易员都在那些不同的城市工作过
	        List<String> cities =
	                transactions.stream()
	                        .map(transaction -> transaction.getTrader().getCity())
	                        .distinct()
	                        .collect(toList());
	        System.out.println(cities);

	        // 问题三： 找出所有来自剑桥的交易员，并按姓名排序	        
//	        List<Trader> traders =
//	                transactions.stream()
//	                        .map(Transaction::getTrader)
//	                        .filter(trader -> trader.getCity().equals("Cambridge"))
//	                        .distinct()
//	                        .sorted(comparing(Trader::getName))
//	                        .collect(toList());
	        // 方式二
	        List<Trader> traders =
	                transactions.stream()
	                        .map(t -> t.getTrader())
	                        .filter(trader -> trader.getCity().equals("Cambridge"))
	                        .distinct()
	                        .sorted((t1, t2) -> t1.getName().compareTo(t1.getName()))
	                        .collect(toList());
	        System.out.println(traders);


	        // 问题四： 返回所有的交易员的姓名，按字母顺序排序
	        String traderStr =
	                transactions.stream()
	                        .map(transaction -> transaction.getTrader().getName())
	                        .distinct()
	                        .sorted()
	                        .reduce("", (n1, n2) -> n1 + n2);
	        System.out.println(traderStr);

	        // Query 5: Are there any trader based in Milan?

	        boolean milanBased =
	                transactions.stream()
	                        .anyMatch(transaction -> transaction.getTrader()
	                                .getCity()
	                                .equals("Milan")
	                        );
	        System.out.println(milanBased);


	        // Query 6: Update all transactions so that the traders from Milan are set to Cambridge.
	        transactions.stream()
	                .map(Transaction::getTrader)
	                .filter(trader -> trader.getCity().equals("Milan"))
	                .forEach(trader -> trader.setCity("Cambridge"));
	        System.out.println(transactions);


	        // Query 7: What's the highest value in all the transactions?
	        int highestValue =
	                transactions.stream()
	                        .map(Transaction::getValue)
	                        .reduce(0, Integer::max);
	        System.out.println(highestValue);
	    }
}
