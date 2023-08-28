package com.rock.hadoop.core.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class CountSpark {
    private static final Pattern SPACE = Pattern.compile("");



    public static void main(String[] args) throws Exception {
        String osType = System.getProperty("os.name");
        System.out.println(osType);
        String[] paths=new String[2];
        paths[0]=osType.contains("Mac")?"/Users/opayc/products/hadoop/conf/int1.txt":"D:\\opayProduct\\hadoop\\conf\\int1.txt";
        paths[1]=osType.contains("Mac")?"/Users/opayc/products/hadoop/conf/out/spark":"D:\\opayProduct\\hadoop\\conf\\out\\spark";
        javaWordCount(paths);
    }

    public static void firstOne(){
        // 创建Spark实例
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        //java 上下文
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 读取数据，这里是一个关于Spark介绍的文本
        String filename = "D:\\opayProduct\\hadoop\\conf\\int1.txt";
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
        result.saveAsTextFile("D:\\opayProduct\\hadoop\\conf\\out\\spark");
        // 输出第一个
        List<Tuple2<String, Integer>> resList = result.collect();
        for(Tuple2<String, Integer> tp:resList){
            System.out.println(tp._1+"\t"+tp._2);
        }

        jsc.stop();
    }

    public static void javaWordCount(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        /**
         * 对于所有的spark程序所言，要进行所有的操作，首先要创建一个spark上下文。
         * 在创建上下文的过程中，程序会向集群申请资源及构建相应的运行环境。
         * 设置spark应用程序名称
         * 创建的 sarpkContext 唯一需要的参数就是 sparkConf，它是一组 K-V 属性对。
         */
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
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

        List<String> excludeList = Arrays.asList(":", ",", "，", "。", "《", "》", "？", "！"," ","；");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).stream().filter(c->!excludeList.contains(c)).iterator();
            }
        });
        /**
         * 输出map 键值对 ，类似于MR的map方法
         * pairFunction<T,K,V>: T:输入类型；K,V：输出键值对
         * 表示输入类型为T,生成的key-value对中的key类型为k,value类型为v,对本例,T=String, K=String, V=Integer(计数)
         * 需要重写call方法实现转换
         */
        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            //scala.Tuple2<K,V> call(T t)
            //Tuple2为scala中的一个对象,call方法的输入参数为T,即输入一个单词s,新的Tuple2对象的key为这个单词,计数为1
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        //A two-argument function that takes arguments
        // of type T1 and T2 and returns an R.
        /**
         * 调用reduceByKey方法,按key值进行reduce（减少）
         *  reduceByKey方法，类似于MR的reduce
         *  要求被操作的数据（即下面实例中的ones）是KV键值对形式，该方法会按照key相同的进行聚合，在两两运算
         *  例如：ones有<"one", 1>, <"one", 1>,会根据"one"将相同的pair单词个数进行统计,输入为Integer,输出也为Integer
         *输出<"one", 2>
         *
         *  备注：spark也有reduce方法，输入数据是RDD类型就可以，不需要键值对，
         *  reduce方法会对输入进来的所有数据进行两两运算
         */
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            //reduce阶段，key相同的value怎么处理的问题

            /**
             *
             * @param i1 为相同key的其中一个value
             * @param i2 为相同key的另外一个value
             * @return 分组处理之后的值
             */
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        //保存结果到文件
        counts.saveAsTextFile(args[1]);

        /**
         * 聚合
         * collect方法用于将spark的RDD类型转化为我们熟知的java常见类型
         */
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        ctx.stop();
    }
}
