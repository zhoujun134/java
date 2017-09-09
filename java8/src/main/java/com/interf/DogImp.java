package com.interf;

import java.util.function.Consumer;

/**
 * Created by zhoujun on 2017/9/6.
 */
public class DogImp implements Dog<String>{
    private String name;
    private int age;

    @Override
    public void speark(String msg) {
        log(msg);
    }

    @Override
    public void doSth(Consumer<String> consumer) {
        consumer.accept(name);
    }

    public String getName() {
        return name;
    }

    public DogImp setName(String name) {
        this.name = name;
        return this;
    }

    public int getAge() {
        return age;
    }

    public DogImp setAge(int age) {
        this.age = age;
        return this;
    }

    @Override
    public String toString() {
        return "DogImp{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
