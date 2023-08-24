package com.rock.hadoop.core.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class CountSpark {
    public static void main(String[] args) {
        // 创建Spark实例
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        //java 上下文
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 读取数据，这里是一个关于Spark介绍的文本
        String filename = "/Users/opayc/products/hadoop/conf/int1.txt";
        JavaRDD<String> data = jsc.textFile(filename);

        // 切割压平
        JavaRDD<String> dataMap = data.flatMap(t -> Arrays.asList(t.split("，")).iterator());

        // 组合成元组
        JavaPairRDD<String, Integer> dataPair = dataMap.mapToPair(t -> new Tuple2<>(t,1));

        // 分组聚合
        JavaPairRDD<String, Integer> dataAgg = dataPair.reduceByKey((w1,w2) -> w1+w2);

        // 交换key，再排序
        JavaPairRDD<Integer, String> dataSwap = dataAgg.mapToPair(tp -> tp.swap());
        JavaPairRDD<Integer, String> dataSort = dataSwap.sortByKey(false);
        JavaPairRDD<String, Integer> result = dataSort.mapToPair(tp -> tp.swap());

        // 保存结果，saveAsTextFile()方法是将RDD写到本地，根据执行task的多少生成多少个文件
        // 输出目录不能预先存在，否则报错
        result.saveAsTextFile("/Users/opayc/products/hadoop/conf/out/spark");
        // 输出第一个
        List<Tuple2<String, Integer>> resList = result.collect();
        for(Tuple2<String, Integer> tp:resList){
            System.out.println(tp._1+"\t"+tp._2);
        }

        jsc.stop();

    }
}
