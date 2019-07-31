package com.youfan.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/4/4 9:39
 */
public class TestHbaseAPI_5 {

    public static void main(String[] args) throws IOException {

        //1获取hbase对象
        Configuration configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.quorum", "192.168.1.102");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = ConnectionFactory.createConnection(configuration);


        Admin admin = connection.getAdmin();

      /*  TableName tableName = TableName.valueOf("emp");

        if (admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("删除成功");
        }*/

    /*    //创建表
        HTableDescriptor tb = new HTableDescriptor(TableName.valueOf("emp1"));

        //创建列族
        HColumnDescriptor cd = new HColumnDescriptor("info");

        tb.addFamily(cd);
        byte[][] bs = new byte[3][];
        bs[0]= Bytes.toBytes("0|");
        bs[1]= Bytes.toBytes("1|");
        bs[2]= Bytes.toBytes("2|");

        admin.createTable(tb,bs);
        System.out.println("创建成功");*/


      /*  //增加数据
        Table table = connection.getTable(TableName.valueOf("emp1"));

        String rowkey = "shangsan";
        rowkey = HBaseUtil.genRegionNum(rowkey, 3);

        Put put = new Put(Bytes.toBytes(rowkey));


        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));

        table.put(put);

        connection.close();

        System.out.println("成功了");*/

        Table table = connection.getTable(TableName.valueOf("emp1"));

        String rowkey = "list";
        rowkey = HBaseUtil.genRegionNum(rowkey, 3);


        Put put = new Put(Bytes.toBytes(rowkey));

        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("18"));
        table.put(put);

        table.close();
        connection.close();
        System.out.println("hahah");

    }
}
