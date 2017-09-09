package com.interf;

import java.util.Date;
import java.util.function.Consumer;

/**
 * Created by zhoujun on 2017/9/6.
 */
public interface Animal<T>{
    void speark(String msg);
    void doSth(Consumer<T> consumer);


    default void log(String msg){
        System.out.println( " 时间:  " + new Date() + "   日志:  " + msg);
    }
}
