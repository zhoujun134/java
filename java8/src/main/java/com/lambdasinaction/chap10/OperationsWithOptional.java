package com.lambdasinaction.chap10;

import java.util.Optional;

import static java.util.Optional.empty;
import static java.util.Optional.of;

public class OperationsWithOptional {

    public static void main(String... args) {
        System.out.println(max(of(3), of(5)));
        System.out.println(max(empty(), of(5)));

//        Optional<Integer> opt1 = of(5);
//        Optional<Integer> opt2;
//
//        System.out.println(
//        of(5).or(() -> of(4))
//                          );
    }

    public static final Optional<Integer> max(Optional<Integer> i, Optional<Integer> j) {
         return i.flatMap(a -> j.map(b -> Math.max(a, b)));
    }
}
