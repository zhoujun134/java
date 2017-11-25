package com.test2data;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SparkIO_ObjectFile {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkIO");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        List<Person> personList  = new ArrayList<>();
        personList.add(new Person("zzz",21));
        personList.add(new Person("xxx",27));
        personList.add(new Person("ccc",25));
        personList.add(new Person("aaa",22));
        personList.add(new Person("sss",21));
        sc.setCheckpointDir("E:\\git\\java\\spark\\src\\main\\resources\\text");


        JavaRDD<Person> rdd = sc.parallelize(personList,1);






        sc.stop();
        sc.close();
    }
}


class Person implements Serializable{
    String name;
    int age;

    public Person() {
    }

    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
