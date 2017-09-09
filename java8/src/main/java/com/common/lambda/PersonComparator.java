package com.common.lambda;

import com.lambda.Person;

import java.util.Comparator;

/**
 * Created by zhoujun on 2017/9/6.
 */
public class PersonComparator implements Comparator<Person> {
    //为两个元素设定比较规则
    public int compare(Person o1, Person o2){
        //Person p1 = (Person)o1;
        //Person p2 = (Person)o2;
        return o1.getId() - o2.getId();
    }
}
