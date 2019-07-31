package com.youfan.bigdata.JVM;

/**
 * Create by chenqinping on 2019/4/17 9:49
 * 单例模式
 */


public class SingletonDemo {

    private static volatile SingletonDemo instance = null;

    private SingletonDemo() {

        System.out.println(Thread.currentThread().getName() + "\t我是单例模式SingletonDemo");

    }

    //DCL (Double Check Lock双端检锁机制)
    public static SingletonDemo getInstance() {
        if (instance == null) {

            synchronized (SingletonDemo.class) {

                if (instance == null) {

                    instance = new SingletonDemo();
                }
            }
        }
        return instance;
    }

    public static void main(String[] args) {
/*
        System.out.println(SingletonDemo.getInstance() == SingletonDemo.getInstance());
        System.out.println(SingletonDemo.getInstance() == SingletonDemo.getInstance());
        System.out.println(SingletonDemo.getInstance() == SingletonDemo.getInstance());*/

        for (int i = 1; i < 10; i++) {
            new Thread(() -> {
                SingletonDemo.getInstance();
            }, String.valueOf(i)).start();
        }
    }
}
