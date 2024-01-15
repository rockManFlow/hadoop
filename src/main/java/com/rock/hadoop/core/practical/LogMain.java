package com.rock.hadoop.core.practical;

import com.alibaba.fastjson.JSON;
import com.rock.hadoop.core.utils.DateTimeUtil;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author rock
 * @detail
 * @date 2024/1/15 16:13
 */
public class LogMain {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("ApiCost").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        /**
         * 利用textFile接口从文件系统中读入指定的文件，返回一个RDD实例对象。
         * RDD的初始创建都是由SparkContext来负责的，将内存中的集合或者外部文件系统作为输入源。
         * RDD：弹性分布式数据集，即一个 RDD 代表一个被分区的只读数据集。一个 RDD 的生成只有两种途径，
         * 一是来自于内存集合和外部存储系统，另一种是通过转换操作来自于其他 RDD，比如 Map、Filter、Join，等等。
         * textFile()方法可将本地文件或HDFS文件转换成RDD，读取本地文件需要各节点上都存在，或者通过网络共享该文件
         *读取一行
         */
        JavaRDD<String> lines = ctx.textFile(args[0], 1);
        /**
         *
         * new FlatMapFunction<String, String>两个string分别代表输入和输出类型
         * Override的call方法需要自己实现一个转换的方法，并返回一个Iterable的结构
         *
         * flatmap属于一类非常常用的spark函数，简单的说作用就是将一条rdd数据使用你定义的函数给分解成多条rdd数据
         * 例如，当前状态下，lines这个rdd类型的变量中，每一条数据都是一行String，我们现在想把他拆分成1个个的词的话，
         * 可以这样写 ：flatMap输出一个一个的，非k-v形式
         */
        //flatMap与map的区别是，对每个输入，flatMap会生成一个或多个的输出，而map只是生成单一的输出
        //用空格分割各个单词,输入一行,输出多个对象,所以用flatMap

        //包括指定信息
        List<String> includeList = Arrays.asList("@P", "@R");
        //过滤需要的信息
        JavaRDD<String> filterRdd = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if (StringUtils.isNotBlank(s) && containsAll(s, includeList)) {
                    return true;
                }
                return false;
            }
        });

        //转成指定实体类rdd
        JavaRDD<LogInfo> logInfoRDD = filterRdd.map(new Function<String, LogInfo>() {
            @Override
            public LogInfo call(String s) throws Exception {
                return JSON.parseObject(s, LogInfo.class);
            }
        });

        //转换成指定数据结构
        JavaPairRDD<String, Long> timeJavaPairRDD = logInfoRDD.mapToPair(new PairFunction<LogInfo, String, Long>() {
            @Override
            public Tuple2<String, Long> call(LogInfo logInfo) throws Exception {
                String uri = "";
                String message = logInfo.getMessage();
                if (StringUtils.isNotBlank(message)) {
                    String[] msgArray = message.split(" ");
                    if (msgArray.length > 0) {
                        uri = msgArray[0];
                    }
                }
                String key = uri + "#"+logInfo.getContextMap();
                LocalDateTime localDateTime = DateTimeUtil.parseStrToUtcTime(logInfo.getLogTime());
                return new Tuple2<String, Long>(key, localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli());
            }
        });

        //计算两个相同请求key对应的请求时间差值
        JavaPairRDD<String, Long> sameReqUriCostRDD = timeJavaPairRDD.reduceByKey(new Function2<Long, Long, Long>() {
            //reduce阶段，key相同的value怎么处理的问题
            /**
             * @param one1 为相同key的其中一个value
             * @param one2 为相同key的另外一个value
             * @return 分组处理之后的值
             */
            @Override
            public Long call(Long one1, Long one2) {
                return (one1-one2)>0?(one1-one2):(one2-one1);
            }
        });


        // 交换key，再排序--元数据key-value进行交换
        JavaPairRDD<Long, String> dataSwap = sameReqUriCostRDD.mapToPair(tp -> tp.swap());
        //通过交换后的value-key通过value进行降序排序
        JavaPairRDD<Long, String> dataSort = dataSwap.sortByKey(false);
        //排完序的元数据，再交换回来
        JavaPairRDD<String, Long> resultSort = dataSort.mapToPair(tp -> tp.swap());

        /**
         * sort.saveAsNewAPIHadoopFile();
         * sort.saveAsHadoopFile();
         */
        //或者保存到 hdfs中。hdfs://localhost:9000/input/infile/test_count_int1.txt
        //保存结果到文件
        resultSort.saveAsTextFile(args[1]);

        /**
         * 聚合
         * collect方法用于将spark的RDD类型转化为我们熟知的java常见类型
         */
        List<Tuple2<String, Long>> output = resultSort.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        ctx.stop();
    }

    private static Boolean containsAll(String line,List<String> list){
        if(CollectionUtils.isNotEmpty(list)){
            for(String containsKey:list){
                boolean result = line.contains(containsKey);
                if(Boolean.TRUE.equals(result)){
                    return true;
                }
            }
        }
        return false;
    }

    @Data
    public static class LogInfo{
        private String logTime;
        private String message;
        private String level;
        private String contextMap;
        private String traceId;
    }
}
