package com.youfan.bigdata.JVM;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Create by chenqinping on 2019/4/17 14:26
 */
@Getter
@Setter
@ToString
class User {
    String userName;
    int age;


    public User(String userName, int age) {
        this.userName = userName;
        this.age = age;
    }
}

public class AtomicReferenceDemo {

    public static void main(String[] args) {

        User z3 = new User("z3", 23);
        User lisi = new User("lisi", 23);

        AtomicReference<User> objectAtomicReference = new AtomicReference<>();
        objectAtomicReference.set(z3);

        System.out.println(objectAtomicReference.compareAndSet(z3, lisi) + "\t" + objectAtomicReference.get().toString());
        System.out.println(objectAtomicReference.compareAndSet(z3, lisi) + "\t" + objectAtomicReference.get().toString());
    }
}
