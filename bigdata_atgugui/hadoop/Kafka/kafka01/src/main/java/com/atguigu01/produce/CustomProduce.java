package com.atguigu01.produce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Create by chenqinping on 2019/3/27 09 00
 */
public class CustomProduce {

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.ACKS_CONFIG, "all");

//        properties.put(ProducerConfig.RETRIES_CONFIG, 1);//���Դ���

        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);//���δ�С

        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);//�ȴ�ʱ��
//        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);//RecordAccumulator��������С


      /*  List<String> list = new ArrayList<>();
        list.add("com.atguigu.interceptor.TimeInterceptor");
        list.add("com.atguigu.interceptor.CounterInterceptor");

        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, list);*/
        //����һ�������߶���
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        //����send
        for (int i = 0; i < 1000; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("second", i + "", "message" + i));
        }


        //�ر�

        kafkaProducer.close();

    }
}
