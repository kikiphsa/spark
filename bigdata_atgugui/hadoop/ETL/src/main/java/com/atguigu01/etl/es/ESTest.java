package com.atguigu01.etl.es;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;

/**
 * Create by chenqinping on 2019/6/13 11:04
 */
public class ESTest {

    private TransportClient client;

    @Test
    public void getClient() throws Exception {
        // 1 设置连接的集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-es").build();
        // 2 连接集群
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.102"), 9300));
        // 3 打印集群名称
        System.out.println(client.toString());

    }

    @Test
    public void createIndex_blog(){
        // 1 创建索引
        client.admin().indices().prepareCreate("blog2").get();
        // 2 关闭连接
        client.close();
    }

}
