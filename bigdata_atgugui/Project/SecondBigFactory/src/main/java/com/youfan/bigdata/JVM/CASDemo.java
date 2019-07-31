package com.youfan.bigdata.JVM;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Create by chenqinping on 2019/4/17 10:48
 * 1.CAS是什么 ?
 * 比较并交换
 */
public class CASDemo {

    public static void main(String[] args) {
        AtomicInteger atomicInteger = new AtomicInteger(5);

        System.out.println(atomicInteger.compareAndSet(5, 2019) + "\t" + atomicInteger.get());

        System.out.println(atomicInteger.compareAndSet(2019, 1024) + "\t" + atomicInteger.get());

        atomicInteger.getAndIncrement();
    }
}
