package com.atguigu01.zookeeper;

import org.apache.zookeeper.*;


import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Create by chenqinping on 2019/3/9 08 37
 */
public class ZkClient {

    private ZooKeeper zkCli;

    private static final String CONNECT_STRING = "hadoop102:2181,hadoop103:2181,hadoop104:2181";

    private static final int SESSION_ITEMOUT = 2000;


    @Before
    public void before() throws Exception {

        zkCli = new ZooKeeper(CONNECT_STRING, SESSION_ITEMOUT, event -> {
            System.out.println("默认的");
        });
    }

    @Test
    public void ls() throws Exception {
        List<String> cliChildren = zkCli.getChildren("/", event -> {
            System.out.println("自定义的");

        });

        System.out.println("==================================");

        for (String cliChild : cliChildren) {
            System.out.println(cliChild);
        }
        System.out.println("==================================");

        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void create() throws Exception {
        String s = zkCli.create("/nihao", "nihao123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(s);
    }

    @Test
    public void get() throws Exception {
        byte[] data = zkCli.getData("/nihao", true, new Stat());

        if (data != null) {

            String s = new String(data);
            System.out.println(s);
        }
    }

    @Test
    public void set() throws Exception {
//        Stat exists = zkCli.exists("/nihao", false);
        Stat stat = zkCli.setData("/nihao", "1234".getBytes(), 0);
        System.out.println(stat);

    }

    @Test
    public void delete() throws Exception {

        Stat exists = zkCli.exists("/iii", false);
        if (exists != null) {
            zkCli.delete("/ii", exists.getVersion());
        } else {
            System.out.println(exists.getDataLength());
        }
    }


    public void register() throws KeeperException, InterruptedException {

        byte[] date = zkCli.getData("/ii", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    register();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, null);

        System.out.println(new String(date));
    }

    @Test
    public void getregister() {

        try {
            register();
            Thread.sleep(Long.MAX_VALUE);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
