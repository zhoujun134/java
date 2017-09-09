package com.common.lambda;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by zhoujun on 2017/9/4.
 */
public class ClassTest {
    public static void main(String[] args){
        List<String> list = filter(Arrays.asList("Hello","helli","java", "c#","sca"),
                s -> s.length() > 3);
        list.forEach(System.out :: println);
    }

    public static List<String> filter(List<String> list, Predicate<String> predicate){
        return list.stream()
                .filter(s -> predicate.test(s))
                .collect(Collectors.toList());
    }
}
