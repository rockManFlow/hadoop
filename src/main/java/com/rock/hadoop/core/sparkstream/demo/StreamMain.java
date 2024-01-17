package com.rock.hadoop.core.sparkstream.demo;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 从不同输入源来获取数据--需要引入不同源包
 * 数据可以从Kafka，Flume， Kinesis， 或TCP Socket来源获得
 */
public class StreamMain {
    public static void main(String[] args) throws InterruptedException {

    }

    public static void kafkaStream(String[] args){
        /*
		  node-01:9092,node-02:9092,node-03:9092 directKafka_test
		 */
        if (args.length < 2) {
            System.err.println(
                    "|Usage: DirectKafkaWordCount <brokers> <topics> "
                            + "|  <brokers> is a list of one or more Kafka brokers"
                            + "|  <topics> is a list of one or more kafka topics to consume from"
                            + "|");
            System.exit(1);
        }

        // 构建Spark Streaming上下文
        SparkConf conf = new SparkConf()
                //.setMaster("local[2]")
                .setAppName("Java_DirectKafka")
                // 设置程序优雅的关闭
                .set("spark.streaming.stopGracefullyOnShutdown", "true")
                // 设置spark streaming 从kafka消费数据的最大速率 （每秒 / 每个分区；秒  * 分区  * 值）
                // 生产环境下，测试你集群的计算极限
                .set("spark.streaming.kafka.maxRatePerPartition", "50")
                // 推荐的序列化方式
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // 指定序列化方式
                .set("spark.default.parallelism", "15")         // 调节并行度
                .set("spark.streaming.blockInterval", "200")    // Spark Streaming接收器接收的数据在存储到Spark之前被分块为数据块的时间间隔; 增加block数量，增加每个batch rdd的partition数量，增加处理并行度
                .set("spark.shuffle.consolidateFiles", "true")  // 开启shuffle map端输出文件合并的机制，默认false
                .set("spark.shuffle.file.buffer", "128K")       // 调节map端内存缓冲，默认32K
                .set("spark.shuffle.memoryFraction", "0.3")      // reduce端内存占比，默认0.2
                .set("spark.scheduler.mode", "FAIR")           // 设置调度策略，默认FIFO
                .set("spark.locality.wait", "2")              // 调节本地化等待时长，默认3秒

                // .set("spark.streaming.receiver.maxRate", "1") // receiver方式接收; 限制每个 receiver 每秒最大可以接收的记录的数据
                .set("spark.streaming.kafka.maxRatePerPartition", "2000") // 限流，对目标topic每个partition每秒钟拉取的数据条数 条数/（每个partition）
                .set("spark.streaming.backpressure.enabled", "true") // 启用backpressure机制，默认值false
                .set("spark.streaming.backpressure.pid.minRate", "1"); // 可以估算的最低费率是多少。默认值为 100，只能设置成非负值。//最小摄入条数控制
        //.set("spark.streaming.backpressure.initRate", "1") // 启用反压机制时每个接收器接收第一批数据的初始最大速率。默认值没有设置。
        //.set("spark.streaming.receiver.writeAheadLog.enable", "true");


        // spark streaming的上下文是构建JavaStreamingContext对象
        JavaStreamingContext jssc = new JavaStreamingContext(
                conf, Durations.seconds(2));

        // 创建SparkSession
        SparkSession sparkSession = new SparkSession(jssc.ssc().sc());


        // 创建针对Kafka数据来源的输入DStream（离线流，代表了一个源源不断的数据来源，抽象）
        // 选用kafka direct api（很多好处，包括自己内部自适应调整每次接收数据量的特性，等等）
        String brokers = args[0];
        String topic = args[1];

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

        // Get the lines, split them into words, count the words and print

        JavaDStream<String> wordDStream = messages.map(record -> record.value()).flatMap(line -> {
                    String[] words = line.split(" ");
                    TimeUnit.MILLISECONDS.sleep(2);
                    return Arrays.asList(words).iterator();
                }
        );

        wordDStream.map(word -> new Tuple2<String, Integer>(word, 1)).
                reduce((t1, t2) -> {

                            return new Tuple2<String, Integer>(t1._1, t1._2 + t2._2);
                        }
                ).print();
        // Start the computation

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }

        sparkSession.close();
        jssc.close();
    }

    public static void fileStream(){
        // 使用SparkStreaming
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming02_FileDataSource");
        JavaStreamingContext streamingContext = new JavaStreamingContext(config, Seconds.apply(3)); //3 秒钟，伴生对象，不需要new

        // 从文件夹中采集数据
        JavaDStream<String> fileDStreaming = streamingContext.textFileStream("test");

        // 将采集的数据进行分解（偏平化）
        JavaDStream<String> wordDstream = fileDStreaming.flatMap(
                (String line) -> Arrays.asList(line.split(" ")).iterator() //偏平化后，按照空格分割
        );

        // 将我们的数据进行转换方便分析
        JavaDStream<Tuple2<String, Integer>> mapDstream = wordDstream.map((String word) -> new Tuple2<String, Integer>(word, 1));

        // 将转换后的数据聚合在一起处理
        JavaDStream<Tuple2<String, Integer>> wordToSumStream = mapDstream.reduce((t1, t2) -> new Tuple2(t1._1, t1._2 + t2._2));

        // 打印结果
        wordToSumStream.print();

        // 启动采集器
        streamingContext.start();

        // Driver 等待采集器停止，
        try {
            streamingContext.awaitTermination();
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public static void socketStream(){
        //使用SparkStreaming 完成WordCount
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount");

        JavaStreamingContext streamingContext = new JavaStreamingContext(config, Seconds.apply(3)); //3 秒钟，伴生对象，不需要new

        //从指定的端口中采集数据--从socket源获取数据
        JavaReceiverInputDStream<String> socketLineStreaming = streamingContext.socketTextStream("127.0.0.1", 9999);//一行一行的接受

        //将采集的数据进行分解--拆分成单词
        JavaDStream<String> wordDStream = socketLineStreaming.flatMap( (String line) ->{
            String[] split = line.split(" ");
            return Arrays.asList(split).iterator();
        });

        //计算每个单词出现的个数
        JavaPairDStream<String, Integer> wordCounts = wordDStream.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        //打印结果
        wordCounts.print();

        //启动采集器
        streamingContext.start();

        //Drvier等待采集器停止，
        try {
            streamingContext.awaitTermination();
        } catch (Exception ex){
            ex.printStackTrace();
        }finally {
            streamingContext.close();
        }

        //nc -lc 9999   linux 下往9999端口发数据。
    }
}
