package com.rock.hadoop.core.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * 接入Kafka
 */
public class KafkaConfig {

    public Producer buildProvider(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1"); // 消息确认机制:  all表示 必须等待kafka端所有的副本全部接受到数据 确保数据不丢失
        // 说明: 在数据发送的时候, 可以发送键值对的, 此处是用来定义k v的序列化的类型
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer(props);

        //3. 释放资源
//        producer.close();

        return producer;
    }

    public void buildConsumer(){
        //1.1: 指定消费者的配置信息
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "127.0.0.1:9092");
        props.setProperty("group.id", "group-test"); // 消费者组的名称
        props.setProperty("enable.auto.commit", "true"); // 消费者自定提交消费偏移量信息给kafka
        props.setProperty("auto.commit.interval.ms", "1000"); // 每次自动提交偏移量时间间隔  1s一次
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        /**
         * 组与组间的消费者是没有关系的。
         * topic中已有分组消费数据，新建其他分组ID的消费者时，之前分组提交的offset对新建的分组消费不起作用。
         *
         * earliest
         * 无提交时，各分区的offset从0开始消费。
         * latest
         * 最新开始消费。
         * none
         * 抛出NoOffsetForPartitionException异常。
         */
        props.setProperty("auto.offset.reset","earliest");

        //1. 创建kafka的消费者核心类对象:  KafkaConsumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //2. 让当前这个消费, 去监听那个topic?
        consumer.subscribe(Arrays.asList("test-example")); // 一个消费者 可以同时监听多个topic的操作
        while (true) { // 一致监听
            //3. 从topic中 获取数据操作:  参数表示意思, 如果队列中没有数据, 最长等待多长时间
            // 如果超时后, topic中依然没有数据, 此时返回空的  records(空对象)
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            //4. 遍历ConsumerRecords, 从中获取消息数据
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                System.out.println("接收到消息为:"+value);
            }
        }
    }

    public static void main(String[] args) {
        KafkaConfig kafkaConfig=new KafkaConfig();
        kafkaConfig.buildConsumer();
    }
}
