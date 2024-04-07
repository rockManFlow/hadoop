package com.rock.hadoop.core.practical;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SparkStreamUserBehaviorMain {
    public static void main(String[] args) {

    }

    /**
     * 收集-Kafka-spark stream
     * mq:userId,USER_BEHAVIOR_type
     * 用于实时分析用户的喜好，并给用户推送对应的喜好视频或数据
     */
    public static void check(){
        SparkConf conf = new SparkConf();
        conf.setAppName("Java_DirectKafka");
        conf.setMaster("local[*]");

        JavaStreamingContext jssc = new JavaStreamingContext(
                conf, Durations.seconds(2));

        String brokers = "127.0.0.1:9092";
        String topic = "USER_BEHAVIOR_DATA";

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", topic + "_g1"); //_g1Test 本地测试
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList(topic);

        // 从kafka中的ConsumerRecord获取数据
        // 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
        // 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        //进行数据分析处理

    }
}
