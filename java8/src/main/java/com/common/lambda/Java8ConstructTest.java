package com.common.lambda;

import com.lambda.Employee;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by zhoujun on 2017/9/4.
 */
public class Java8ConstructTest {
    public static void main(String[] arsg) {
        test();
        test1();
        test2();
        test3();
    }

    public static void test(){
        final Employee employee = new Employee();
        Supplier<String> supplier = employee :: getFirstName;
        System.out.println("firsName: " + supplier.get());
    }
    public static void test1(){
        Supplier<Employee> employeeSupplier = Employee::new;
        System.out.println("无参数构造函数创建:  " + employeeSupplier.get());
    }
    public static void test2(){
        Function<String, Employee> emp = Employee::new;
        System.out.println("一个参数构造函数创建:  " + emp.apply("zhang"));
    }
    public static void test3(){
        BiFunction<String, String, Employee> emp = Employee::new;
        System.out.println("两个个参数构造函数创建:  " + emp.apply("zhang","shan"));
    }
    public static void test4(){

    }


}
