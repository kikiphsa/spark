package com.atguigu01.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Create by chenqinping on 2019/3/27 10 29
 */
public class CustomConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("group.id", "test");//�������飬ֻҪgroup.id��ͬ��������ͬһ����������
        props.put("enable.auto.commit", "false");//�Զ��ύoffset
        props.put("auto.commit.interval.ms", "1000");//�ύoffsetʱ����
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("second","first"));
        while (true){

            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value()+record.headers()+record.topic());
            }
            consumer.commitSync();
        }

    }
}
