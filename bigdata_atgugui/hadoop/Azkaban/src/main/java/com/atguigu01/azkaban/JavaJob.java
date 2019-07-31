package com.atguigu01.azkaban;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/30 10 47
 */
public class JavaJob {

    public static void main(String[] args) {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream("/opt/module/azkaban.txt");
            for (int i = 0; i < 100; i++) {
                fos.write("this is java job".getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
