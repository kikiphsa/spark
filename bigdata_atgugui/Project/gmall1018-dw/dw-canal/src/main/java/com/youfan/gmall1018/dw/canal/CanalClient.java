package com.youfan.gmall1018.dw.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Create by chenqinping on 2019/5/3 18:50
 */
public class CanalClient {

    public static void main(String[] args) {

        CanalConnector canalConnector = CanalConnectors.newSingleConnector
                (new InetSocketAddress("hadoop102", 11111), "example", "", "");
        while (true) {
            canalConnector.connect();
            //订阅表
            canalConnector.subscribe("gmall1018.order_info");
            //抓取信息
            Message message = canalConnector.get(100);
            if (message.getEntries().size() == 0) {
                System.out.println("没有数据,休息5分钟");
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : message.getEntries()) {

                    //需要entry storevalue (序列化的内容)
                    //只要行变化内容
                    if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                        continue;
                    }
                    CanalEntry.RowChange rowChange = null;
                    try {
                        rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }

                    //需要rowchange  eventtype(insert,update)   rowdatalist   表名
                    //表名
                    String tableName = entry.getHeader().getTableName();
                    //操作类型
                    CanalEntry.EventType eventType = rowChange.getEventType();
                    //行集
                    List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                    CanalHandler.handle(tableName, eventType, rowDatasList);

                }

            }
        }

    }

}
