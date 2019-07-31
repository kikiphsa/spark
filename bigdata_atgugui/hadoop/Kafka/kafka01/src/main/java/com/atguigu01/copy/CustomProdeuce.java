package com.atguigu01.copy;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Create by chenqinping on 2019/7/20 19:26
 */
public class CustomProdeuce {

    public static void main(String[] args) {


        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        prop.put(ProducerConfig.ACKS_CONFIG,"all");
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        prop.put(ProducerConfig.LINGER_MS_CONFIG,1);


        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("com.atguigu01.interceptor.CounterInterceptor");
        interceptors.add("com.atguigu01.copy.TimeInterceptor");
        prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        KafkaProducer producer = new KafkaProducer(prop);


        for (int i=0;i<100;i++){
            producer.send(new ProducerRecord<String, String>("topic_start", i + "", "message-" + i));
        }

        producer.close();
    }
}
