package com.youfan.bigdata.JVM;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Create by chenqinping on 2019/4/16 20:39
 */

class MyData {
    //volatile 静止指令重排
    volatile int number = 0;

    public void addT060() {
        this.number = 20;
    }

    //请法意，此时number前面是加了volatile关键字修筋前，volatile不保证原子性
    public void addPlusPlus() {
        number++;
    }

    AtomicInteger atomicInteger = new AtomicInteger();

    public void addAtomic(){
        atomicInteger.getAndIncrement();
    }
}

/**
 * 1验 volaTile的可见性
 * 1.1假如 int number=0;, number变量之前根本没有添加 olatile关键字修饰,没有可见性
 * 1.2添加 volatile,可以解决可见性问题。
 * *2验证 volatile不保证原子性
 * 2.1原子性指的是什么意思?
 * 不可分割,完整性,也即某个线程正在做某个具体业务时,中间不可以酸加塞或者被分别。而要整体完整
 * 要么同时成功,要么同时失败
 * 2. volatile不保证原子性的案例演示
 */

public class VolatioeDemo {

    public static void main(String[] args) {

        MyData myData = new MyData();

        for (int i = 1; i <= 20; i++) {

            new Thread(() -> {
                for (int j = 1; j <= 1000; j++) {
                    myData.addPlusPlus();
                    myData.addAtomic();
                }

            }, String.valueOf(i)).start();
        }

        while (Thread.activeCount() > 2) {
            Thread.yield();
        }



        System.out.println(Thread.currentThread().getName() + "\t number" + myData.number);
        System.out.println(Thread.currentThread().getName() + "\t number" + myData.atomicInteger);

    }
}
