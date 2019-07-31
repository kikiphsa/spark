package com.youfan.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/4/4 9:39
 */
public class TestHbaseAPI_2 {

    public static void main(String[] args) throws IOException {

        //1获取hbase对象
        Configuration configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.quorum", "192.168.1.102");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = ConnectionFactory.createConnection(configuration);

        TableName tableName = TableName.valueOf("atguigu:student");

        Admin admin = connection.getAdmin();

        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("删除成功");
        }

       //删除列族
        /*Table table = connection.getTable(tableName);
        String roekey="1001";
        Delete delete = new Delete(Bytes.toBytes(roekey));

        table.delete(delete);*/


        //扫描数据
     /*   Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        table.getScanner(scan);*/
    }
}
