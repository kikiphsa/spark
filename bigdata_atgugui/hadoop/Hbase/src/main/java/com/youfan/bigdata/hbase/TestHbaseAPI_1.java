package com.youfan.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/4/4 9:39
 */
public class TestHbaseAPI_1 {

    public static void main(String[] args) throws IOException {

        //1获取hbase对象
        Configuration configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.quorum", "192.168.1.102");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = ConnectionFactory.createConnection(configuration);


//        System.out.println(connection);

        //2获取操作对象
//        new HBaseAdmin()
        Admin admin = connection.getAdmin();

        try {
            admin.getNamespaceDescriptor("atguigu");
        } catch (IOException e) {
            NamespaceDescriptor build = NamespaceDescriptor.create("atguigu").build();
            admin.createNamespace(build);

        }


        //3操作数据库
        TableName tableName = TableName.valueOf("student");
        boolean student = admin.tableExists(tableName);

        if (student) {
//            admin.
            //获取指定的表对象
            Table table = connection.getTable(tableName);
            //查询数据
            String rowkey = "1001";
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            boolean empty = result.isEmpty();
            System.out.println("数据是否存在=" + !empty);

            if (empty) {
                //新增数据
                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
                table.put(put);
            } else {
                //展示数据
                for (Cell cell : result.rawCells()) {
                    byte[] cloneRow = CellUtil.cloneRow(cell);
                    System.out.println("cloneRow =" + Bytes.toString(cloneRow));
                    byte[] family = CellUtil.cloneFamily(cell);
                    System.out.println("family =" + Bytes.toString(family));
                    byte[] bytes = CellUtil.cloneValue(cell);
                }
            }

        } else {

            //创建表
            HTableDescriptor tb = new HTableDescriptor(tableName);

            // 增加协处理器
            tb.addCoprocessor("com.atguigu.bigdatahbase.InsertAtguiguStudentCoprocesser");

            //创建列族
            HColumnDescriptor info = new HColumnDescriptor("info");
            tb.addFamily(info);
            admin.createTable(tb);
            System.out.println("创建成功");
        }
        //4获取操作结果

        //5 关闭数据库连接


    }
}
