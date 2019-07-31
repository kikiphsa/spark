package com.atguigu01.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Create by chenqinping on 2019/3/27 14 29
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    private long successNum = 0l;
    private long errorNum = 0l;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

        if (exception == null) {
            successNum++;
        } else {
            errorNum++;
        }
    }

    @Override
    public void close() {
        System.out.println("successNum: " + successNum);
        System.out.println("errorNum: " + errorNum);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
