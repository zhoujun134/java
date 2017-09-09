package com.common.lambda;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class ComputeTest {
    public static void main(String[] args){
        //计算和
        Compute<Integer, Integer> computeHe = (x, y) -> x +y;
        System.out.println("和： " + computeHe.getValue(2, 6));

        //计算乘积
        Compute<Integer, Integer> computeJi = (x, y) -> x*y;
        System.out.println("乘积： " + computeJi.getValue(2, 6));

        //计算绝对值
        Compute<Integer, Integer> computeAbs = (x, y) -> (x - y) < 0 ? (y - x) : (x - y);
        System.out.println("绝对值： " + computeAbs.getValue(2,6));

        Consumer<String> consumer = (s) -> System.out.println(s);

        Supplier<String> supplier = () -> "hello ";

        Function<Integer, String> function  = x -> x + "";

        Predicate<Integer> predicate = x -> x > 0 ;
    }
}

interface Compute <T, R> {
    R getValue(T x, T y);
}