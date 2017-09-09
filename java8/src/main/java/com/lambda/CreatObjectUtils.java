package com.lambda;

import java.util.ArrayList;
import java.util.Date;
import java.util.function.Consumer;

/**
 * Created by zhoujun on 2017/9/6.
 */
public class CreatObjectUtils<T>{
    private volatile ArrayList<T> arr = new ArrayList<>();

    public CreatObjectUtils<T> add(T t){
        arr.add(t);
        log("add a elements........");
        return this;
    }

    public CreatObjectUtils<T> remove(int index){
        arr.remove(index);
        log("remove a elements.........");
        return this;
    }

    public void audit(T t, Consumer<T> consumer){
        log("audit something........");
        consumer.accept(t);
    }

    public static void log(String masg){
        System.out.println( " 时间:  " + new Date() + "   日志:  " + masg);
    }

    public ArrayList<T> getArr() {
        return arr;
    }
}
