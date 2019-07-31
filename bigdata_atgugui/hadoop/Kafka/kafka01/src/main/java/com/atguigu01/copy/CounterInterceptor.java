package com.atguigu01.copy;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Create by chenqinping on 2019/7/20 19:23
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    private Long success;
    private Long error;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            success++;
        } else {
            error++;
        }

    }

    @Override
    public void close() {
        System.out.println("success"+success);
        System.out.println("error"+error);

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
