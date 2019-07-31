package com.youfan.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/4/4 15:24
 */
public class HBaseUtil {

    private HBaseUtil() {

    }

    private static Connection connection = null;

    public static Connection getHBaseConnection() throws IOException {


        Configuration configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.quorum", "192.168.1.102");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");


        connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    public static void insertData(String tableName, String rowkey, String family, String colum, String value) {

//        connection.
    }


    public static void close() {
        if (connection != null) {
            connection.isClosed();
        }
    }

    //生产分区键
    public static byte[][] genRegionKeys(int regionCount) {
        byte[][] bs = new byte[regionCount - 1][];
        for (int i = 0; i < regionCount - 1; i++) {
            bs[i] = Bytes.toBytes(i + "|");
        }
        return bs;
    }

    //生产分区号
    public static String genRegionNum(String rowkey, int regionCount) {

        int regionNum;
        int hash = rowkey.hashCode();
        if (regionCount > 0 && (regionCount & (regionCount - 1)) == 0) {
            //2 n 次方
            regionNum = hash & (regionCount - 1);

        } else {
            regionNum = hash % (regionCount);
        }
        return regionNum + "_" + rowkey;
    }

    public static void main(String[] args) {
        String zhangsan = genRegionNum("lisi", 3);

        System.out.println(zhangsan);

    }
}
