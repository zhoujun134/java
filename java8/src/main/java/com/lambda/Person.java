package com.lambda;

/**
 * Created by zhoujun on 2017/9/6.
 */
public class Person {
    private String name;
    private int id;

    public Person(String name, int id){
        this.name = name;
        this.id = id;
    }

    @Override
    public String toString(){
        return name + " " + id;
    }

    public boolean equals(Object another){
        //System.out.println("equals()");
        if(another == this){
            return true;
        }
        if(another instanceof Person){
            Person p = (Person)another;
            if(this.name.equals(p.name) == false){
                return false;
            }else if(this.id != p.id){
                return false;
            }else {
                return true;
            }
        }else {
            return false;
        }
    }

    @Override
    public int hashCode(){
        int hash = (name.hashCode() + id) * 31 - 56;
        //System.out.println("hash== " + hash);
        return hash;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
