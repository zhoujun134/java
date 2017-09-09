package com.lambda;

import java.util.TreeSet;

/**
 * Created by zhoujun on 2017/9/6.
 */
public class TreeSetTest_Comparator { public static void main(String[ ] ags){
/*
		TreeSet<String> ts = new TreeSet<String>();
		ts.add("Java");
		ts.add("Hello");
		ts.add("Scala");
		ts.add("Python");
*/
/*
		TreeSet<Person> ts = new TreeSet<Person>();
		ts.add(new Person("Java", 22));
		ts.add(new Person("Hello", 21));
		ts.add(new Person("Scala", 24));
		ts.add(new Person("Python", 18));
		for(Person s : ts){
			System.out.println(s);
		}
*/
    TreeSet<Person> ts = new TreeSet<Person>(
            (x, y) -> {
                Person p1 = (Person) x;
                Person p2 = (Person) y;
                if(p1.getId() == p2.getId()) return p1.getName().compareTo(p2.getName());
                else return p1.getId() - p2.getId();
            });
    ts.add(new Person("Java", 22));
    ts.add(new Person("Hello", 13));
    ts.add(new Person("Scala", 24));
    ts.add(new Person("Scala1", 24));
    ts.add(new Person("Scala2", 24));
    ts.add(new Person("Scala3", 35));
    ts.add(new Person("Python", 18));
    ts.forEach(person -> System.out.println(person));

}

}
