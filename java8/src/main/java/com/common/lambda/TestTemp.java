package com.common.lambda;

import com.lambda.CreatObjectUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Created by zhoujun on 2017/9/6.
 */
public class TestTemp {
    public static void main(String[] args){
        List<String> list = Arrays.asList("ccc","bbb","ddd","aaa");

        list.stream()
                .sorted((s1, s2) -> -s1.compareTo(s2))
                .forEach(System.out :: println);

        System.out.println("***********************************");
        list.stream()
                .sorted((s1, s2) -> s2.compareTo(s1))
                .forEach(System.out :: println);

        System.out.println("***********************************");
        list.stream()
                .sorted((s1, s2) ->{
                    int len1 = s1.toCharArray().length;
                    int len2 = s2.toCharArray().length;
                    int lim = Math.min(len1, len2);
                    char v1[] = s1.toCharArray();
                    char v2[] = s2.toCharArray();
                    int k = 0;
                    while (k < lim) {
                        char c1 = v1[k];
                        char c2 = v2[k];
                        if (c1 != c2) {
                            return c1 - c2;
                        }
                        k++;
                    }
                    return len2 - len1;
                })
                .forEach(System.out :: println);

        System.out.println("zhansghsn");
        CreatObjectUtils<Integer> integerCreatObjectUtils = new CreatObjectUtils<>();
        integerCreatObjectUtils.add(21).add(1212).add(90).add(100).add(1000)
                .audit(2, s-> System.out.println(s));
    }
}
