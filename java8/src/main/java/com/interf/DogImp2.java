package com.interf;

import java.util.function.Consumer;

/**
 * Created by zhoujun on 2017/9/6.
 */
public abstract class DogImp2<T> implements Dog<T>{
    public DogImp2<? super T> dogImp2(Consumer<T> consumer){
        return this;
    }
}
