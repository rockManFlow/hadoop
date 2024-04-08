package com.rock.hadoop.core.practical;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * 模拟进行用户行为数据实时分析并供推荐系统使用
 */
public class SparkStreamUserBehaviorMain {
    public static void main(String[] args) {

    }

    /**
     * 用于实时分析用户的喜好，并给用户推送对应的喜好视频或数据
     *
     * 收集-》Kafka-》spark stream（实时分析用户行为数据统计）-》保存供推荐系统使用
     *
     * mq:userId,USER_BEHAVIOR_type,infoType,xxxx(other type)
     */
    public static void behaviorAnalyse() throws InterruptedException {
        //1创建sparkConf
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkStreamingKafka_Direct");
        conf.setMaster("local[*]");

        //2、创建sparkContext
//        SparkContext sc = new SparkContext(conf);
//        sc.setLogLevel("WARN");

        //3、创建StreamingContext,30s拉取一次
        JavaStreamingContext ssc = new JavaStreamingContext(conf,Durations.seconds(30));

        String brokers = "127.0.0.1:9092,127.0.0.1:9092";
        String topic = "USER_BEHAVIOR_DATA";

        //4、配置kafka相关参数
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);//??metadata.broker.list
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", topic + "_g1"); //本地测试
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

        //5、定义topic
        Collection<String> topics = Arrays.asList(topic);

        //  配置对应的主题分区的offset，从指定offset消费
//        Map<TopicPartition,Long> topicsAndOffset = new HashMap<>();
//        topicsAndOffset.put(new TopicPartition(topic,0),0L);
//        topicsAndOffset.put(new TopicPartition(topic,1),10L);
//        topicsAndOffset.put(new TopicPartition(topic,2),330L);

        //6、通过 KafkaUtils.createDirectStream接受kafka数据，这里采用是kafka低级api偏移量不受zk管理
        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(//需要自己来保存已经消费的分区及offset信息
                        ssc,
                        LocationStrategies.PreferConsistent(),//分区策略
                        ConsumerStrategies.Subscribe(topics, kafkaParams)//消费者策略   方式一 从每个分区的 0 offset 开始消费
                        //ConsumerStrategies.Subscribe( topics,kafkaParams,topicsAndOffset) //消费者策略   方式二 从每个分区的 指定的offset 开始消费
                );


        //7、获取kafka中topic中的数据
        JavaDStream<String> topicData = messages.map(record -> {
            String topic1 = record.topic();
            String value = record.value();
            System.out.println("topic1:" + topic1 + "|value:" + value);
            return value;
        });
        JavaDStream<String> filterDStream = topicData.filter(message -> {
            if (StringUtils.isBlank(message)) {
                return false;
            }
            String[] messageArray = message.split(",");
            if (messageArray.length <= 1) {
                return false;
            }
            //userId,behaviorType,otherType
            if (messageArray.length > 1 && !messageArray[1].equals("VIDEO_DY")) {
                return false;
            }
            return true;
        }).flatMap(record->{
            String[] split = record.split(",");
            List<String> dataList=new ArrayList<>();
            String userId=split[0];
            for(int i=2;i<split.length;i++){
                dataList.add(userId+"#"+split[i]);
            }
            return dataList.iterator();
        });

        JavaPairDStream<String, Long> keyCountDStream = filterDStream.mapToPair(record -> {
            return new Tuple2<String, Long>(record, 1L);
        });
        //把相同的key值进行累加，计算次数
        JavaPairDStream<String, Long> addCountDStream = keyCountDStream.reduceByKey((count1, count2) -> count1 + count2);

        //转换成userId-type1:count1,type2:count2,xxxx
        JavaPairDStream<String, String> resultDStream = addCountDStream.mapToPair(countKey -> {
            String[] keyArray = countKey._1.split("#");
            return new Tuple2<String, String>(keyArray[0], keyArray[1] + ":" + countKey._2);
        }).reduceByKey((typeCount1, typeCount2) -> typeCount1 + "," + typeCount2);

        //10、打印输出或者保存在db/hbase中
        resultDStream.print();
        //开启计算
        ssc.start();
        ssc.awaitTermination();

        //进行数据分析处理
        /**
         * 分析数据：
         * 1、记录用户属性记录次数，之后推荐算法根据占比来随机推送指定类型的信息
         * 2、这些统计之后的数据如何来存储--hive或hbase
         */


    }
}
