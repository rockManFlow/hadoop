package com.rock.hadoop.core.sparkstream.demo;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.dstream.InputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Function1;
import scala.Tuple2;

import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 从不同输入源来获取数据--需要引入不同源包
 * 数据可以从Kafka，Flume， Kinesis， 或TCP Socket，HDFS（Hadoop分布式文件系统，Spark Streaming可以将数据从HDFS中读取并处理为实时数据流）来源获得。
 * 这些都是目前已有包支持的
 *
 * Custom Stream Sources：除了上述提到的数据源外，Spark Streaming还支持自定义数据源的开发，这意味着可以从任何提供特定接口的数据源中获取实时数据。
 *
 */
@Slf4j
public class StreamMain {
    public static void main(String[] args) throws InterruptedException {
        fileStream();
    }

    /**
     * Spark Streaming从Kafka获取数据进行分析相比直接使用普通方式进行分析有以下优势：
     *
     * 实时性：Spark Streaming可以从Kafka中实时获取数据，并进行实时处理和分析。这对于许多应用来说是非常重要的，特别是那些需要快速响应或实时监控的场景。
     * 分布式处理：Spark Streaming可以利用Spark的分布式计算能力，对大规模数据进行高效的处理和分析。通过将数据分片并在多个节点上并行处理，可以大大提高处理速度和吞吐量。
     * 容错性：Spark Streaming具有较好的容错机制，能够自动处理失败的任务和节点，确保数据的可靠性和系统的稳定性。
     * 易于集成：Kafka与Spark Streaming的集成非常方便，提供了丰富的API和工具，简化了数据流的开发和维护工作。
     * 灵活性：Spark Streaming可以与其他Spark组件（如Spark SQL、Spark MLlib等）无缝集成，方便进行复杂的数据分析和机器学习任务。
     * 高效的数据处理：Spark Streaming能够高效地处理数据，减少了数据的冗余和重复处理，提高了数据处理效率。
     * 可扩展性：随着数据规模的增长，Spark Streaming可以方便地扩展计算资源，支持更大规模的数据处理和分析。
     * 综上所述，通过Spark Streaming从Kafka获取数据进行分析可以提供更高的实时性、分布式处理能力、容错性、灵活性、数据处理效率和可扩展性等方面的优势。
     *
     * 通过Receiver模式 又称kafka高级api模式，来与Kafka整合。
     * 效果：SparkStreaming中的Receivers，恰好Kafka有发布/订阅 ，然而：此种方式企业不常用，说明有BUG，不符合企业需求。
     * 因为：接收到的数据存储在Executor的内存，会出现数据漏处理或者多处理状况 简单的理解就是kafka把消息全部分装好，
     * 提供给spark去调用，本来kafka的消息分布在不同的partition上面，相当于做了一步数据合并，在发送给spark，
     * 故spark可以设置excutor个数去消费这部分数据，效率相对慢一些
     *
     * Direct模式 又称kafka低级api模式
     * 需要自己维护取的位移数等信息
     */
    public static void kafkaStream(String[] args){
        /*
		  node-01:9092,node-02:9092,node-03:9092 directKafka_test
		 */
        if (args.length < 2) {
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

    /**
     * 具体的需要测，好像会定期扫描文件夹下的文件内容是否变更，变更，会全部重新计算一遍，具体详情需要进一步实践
     * 作业不会重复读取同一个文件，会根据最后修改时间判断是否读取。如果最后修改时间不一致，会重头开始读取
     *
     * 所以：日志文件的实时处理建议采用flume+Kafka+spark Streaming更常用
     */
    public static void fileStream(){
        // 使用SparkStreaming
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming02_FileDataSource");
        JavaStreamingContext streamingContext = new JavaStreamingContext(config, Seconds.apply(3)); //3 秒钟，伴生对象，不需要new

        // 从文件夹中采集数据
        JavaDStream<String> fileDStreaming = streamingContext.textFileStream("D:\\opayProduct\\hadoop\\data");

        //将采集的数据进行分解--拆分成单词
        JavaDStream<String> wordDStream = fileDStreaming.flatMap( (String line) ->{
            String[] split = line.split("");
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

        // 打印结果
        wordCounts.print();

        // 启动采集器
        streamingContext.start();

        // Driver 等待采集器停止，
        try {
            streamingContext.awaitTermination();
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }

    /**
     * ok
     */
    public static void socketStream(){
        //使用SparkStreaming 完成WordCount
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount");

        //3 秒钟当做一个时间间隔，取一次数据（接收的数据缓存三秒的内容）
        JavaStreamingContext streamingContext = new JavaStreamingContext(config, Seconds.apply(3));

        //之后的处理逻辑是处理这三秒内的数据

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
            log.info("spark streaming start");
            streamingContext.awaitTermination();
        } catch (Exception ex){
            ex.printStackTrace();
        }finally {
            streamingContext.close();
        }

        //nc -lc 9999   linux 下往9999端口发数据。
    }

    public static void logFileStream() throws InterruptedException {
        String appName = "LogAnalysis";
        int batchIntervalSeconds = 5;

        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(batchIntervalSeconds));

        //从hdfs中来读取数据
        JavaDStream<String> logLines = ssc.textFileStream("hdfs://master:9000/user/spark/logs");

        JavaPairDStream<String, Integer> ipCounts = logLines.flatMapToPair((line) -> {
            String[] fields = line.split(" ");
            if (fields.length >= 8 && fields[7].startsWith("/")) {
                return Arrays.asList(new Tuple2<>(fields[0], 1)).iterator();
            } else {
                return Collections.<Tuple2<String,Integer>>emptyIterator();
            }
        }).reduceByKeyAndWindow((a, b) -> a + b, Durations.minutes(10), Durations.minutes(1));

        ipCounts.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
