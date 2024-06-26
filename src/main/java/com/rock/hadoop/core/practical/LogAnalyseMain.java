package com.rock.hadoop.core.practical;

import com.alibaba.fastjson.JSON;
import com.rock.hadoop.core.utils.DateTimeUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * @author rock
 * @detail 日志分析，挑选日志中指定信息并统计
 * 日志示例：2024-05-22T00:00:02.999 [256621110643605] </api/v3/cashier/initialize>, <0>, <138ms>, xxx
 */
@Slf4j
public class LogAnalyseMain {
    public static void main(String[] args) {
        long start=System.currentTimeMillis();
        checkUriArgCost(null);
        System.out.println("cost:"+(System.currentTimeMillis()-start));
    }

    /**
     * 统计商户ID并统计
     * @param args
     */
    public static void checkUriArgCost(String[] args){
        args=new String[]{"D:\\opay-card-web-2024-01-14-1\\opay-card-web-2024-01-14-1","D:\\opayProduct\\hadoop\\conf\\resultArg4"};
        SparkConf sparkConf = new SparkConf().setAppName("ApiArgCost").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        /**
         *读取一行
         */
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        //转成指定实体类rdd
        JavaRDD<String> logInfoRDD = lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                try {
                    if(StringUtils.isNotBlank(s)){
                        return s.split(" ")[1];
                    }
                }catch (Exception e){
                    log.error("parseObject error",e);
                }
                return null;
            }
        });

        //统计商户次数
        JavaPairRDD<String, Integer> timeJavaPairRDD = logInfoRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String merchantId) throws Exception {
                if(StringUtils.isNotBlank(merchantId)){
                    merchantId=merchantId.replace("[","").replace("]","");
                }
                return new Tuple2<String, Integer>(merchantId,1);
            }
        });

        //计算两个相同请求key对应的求和
        JavaPairRDD<String, Integer> sameReqUriCostRDD = timeJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            //reduce阶段，key相同的value怎么处理的问题
            @Override
            public Integer call(Integer one1, Integer one2) {
                return one1+one2;
            }
        });


        // 交换key，再排序--元数据key-value进行交换
        JavaPairRDD<Integer, String> dataSwap = sameReqUriCostRDD.mapToPair(tp -> tp.swap());
        //通过交换后的value-key通过value进行降序排序-单独耗时降序排序
        JavaPairRDD<Integer, String> dataSort = dataSwap.sortByKey(false);
        //排完序的元数据，再交换回来
        JavaPairRDD<String, Integer> resultSort = dataSort.mapToPair(tp -> tp.swap());

        //保存结果到文件夹-.coalesce(1)或者.repartition(1)输出到一个文件中--验证是OK的
        resultSort.coalesce(1).saveAsTextFile(args[1]);

        ctx.stop();
    }

    /**
     * 处理所有URI的耗时
     * @param args
     */
    public static void checkAllUriCost(String[] args){
        args=new String[]{"D:\\opay-card-web-2024-01-14-1\\opay-card-web-2024-01-14-1","D:\\opayProduct\\hadoop\\conf\\result2"};
        SparkConf sparkConf = new SparkConf().setAppName("ApiCost").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        /**
         *读取一行
         */
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

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
                try {
                    return JSON.parseObject(s, LogInfo.class);
                }catch (Exception e){
                    log.error("parseObject error",e);
                }
                return null;
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
                LocalDateTime localDateTime =null;
                try {
                    localDateTime = DateTimeUtil.parseStrToUtcTime(logInfo.getLogTime());
                }catch (DateTimeParseException e){
                    localDateTime = DateTimeUtil.parseStrToTime(logInfo.getLogTime(),DateTimeUtil.UTC_PATTERN_2);
                }
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

        //过滤非指定格式数据
        JavaPairRDD<String, Long> filterIllegalDataRDD = sameReqUriCostRDD.filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                if (stringLongTuple2._2 > 1000000L || stringLongTuple2._2 <= 0) {
                    return false;
                }
                return true;
            }
        });


        // 交换key，再排序--元数据key-value进行交换
        JavaPairRDD<Long, String> dataSwap = filterIllegalDataRDD.mapToPair(tp -> tp.swap());
        //通过交换后的value-key通过value进行降序排序
        JavaPairRDD<Long, String> dataSort = dataSwap.sortByKey(false);
        //排完序的元数据，再交换回来
        JavaPairRDD<String, Long> resultSort = dataSort.mapToPair(tp -> tp.swap());

        /**
         * sort.saveAsNewAPIHadoopFile();
         * sort.saveAsHadoopFile();
         */
        //或者保存到 hdfs中。hdfs://localhost:9000/input/infile/test_count_int1.txt
        //保存结果到文件夹
        resultSort.saveAsTextFile(args[1]);

//        /**
//         * 聚合
//         * collect方法用于将spark的RDD类型转化为我们熟知的java常见类型
//         */
//        List<Tuple2<String, Long>> output = resultSort.collect();
//        for (Tuple2<?,?> tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }
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

    public static class LogComparator implements Comparator<String>, Serializable{
        @Override
        public int compare(String value1, String value2) {
            float avgCost1=Float.valueOf(value1.split("#")[0]);
            float avgCost2=Float.valueOf(value2.split("#")[0]);
            if(avgCost1>avgCost2){
                return 1;
            }else if (avgCost1<avgCost2){
                return -1;
            }
            return 0;
        }
    }
}
