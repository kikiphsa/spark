package com.youfan.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/4/4 9:39
 */
public class TestHbaseAPI_4 {

    public static void main(String[] args) throws IOException {
        //1获取hbase对象
        Configuration configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.quorum", "192.168.1.102");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = ConnectionFactory.createConnection(configuration);

        TableName tableName = TableName.valueOf("student");

        Table table = connection.getTable(tableName);


        Scan scan = new Scan();
        //scan.addFamily(Bytes.toBytes("info"));
        BinaryComparator bc = new BinaryComparator(Bytes.toBytes("1003"));
        RegexStringComparator rsc = new RegexStringComparator("^\\d{3}$");
        Filter f = new RowFilter(CompareFilter.CompareOp.EQUAL, rsc);

        RowFilter rf = new RowFilter(CompareFilter.CompareOp.EQUAL, bc);

        //扫描时,增加过滤器
//        scan.setFilter();

        scan.setFilter(rf);
        ResultScanner scanner = table.getScanner(scan);


        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("values" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("rowkey"+Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("column"+Bytes.toString(CellUtil.cloneFamily(cell)));
            }
        }

        table.close();
        connection.close();


    }
}
