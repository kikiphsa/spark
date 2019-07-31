package com.atguigu01.produce;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Create by chenqinping on 2019/3/27 10 07
 */
public class InterceptorProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");//kafka��Ⱥ��broker-list
        props.put("acks", "all");
        props.put("retries", 1);//���Դ���
        props.put("batch.size", 16384);//���δ�С
        props.put("linger.ms", 1);//�ȴ�ʱ��
        props.put("buffer.memory", 33554432);//RecordAccumulator��������С
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i=0;i<100;i++){
            producer.send(new ProducerRecord<String, String>("first",i+"","message"+i)).get();

            System.out.println(i);
        }


        producer.close();
    }
}
