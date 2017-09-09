package com.lambda;

import com.interf.Pander;

import java.util.function.Consumer;

/**
 * Created by zhoujun on 2017/9/6.
 */
public class PanderImp implements Pander<String> {
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

    public PanderImp setName(String name) {
        this.name = name;
        return this;
    }

    public int getAge() {
        return age;
    }

    public PanderImp setAge(int age) {
        this.age = age;
        return this;
    }

    @Override
    public String toString() {
        return "PanderImp{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
