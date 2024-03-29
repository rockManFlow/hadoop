package com.rock.hadoop.core.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 计算指定文件中各个字符出现的次数并从大到小排序输出
 */
public class CountSpark {
    private static final Pattern SPACE = Pattern.compile("");

    public static void main(String[] args) throws Exception {
        String osType = System.getProperty("os.name");
        System.out.println(osType);
        String[] paths=new String[2];
        paths[0]=osType.contains("Mac")?"/Users/opayc/products/hadoop/conf/int1.txt":"D:\\opayProduct\\hadoop\\conf\\int1.txt";
        paths[1]=osType.contains("Mac")?"/Users/opayc/products/hadoop/conf/out/spark":"D:\\opayProduct\\hadoop\\conf\\out\\spark";
//        javaWordCount(paths);

//        sortOperate();

        //连接hdfs中文件--OK
//        statisticsWordCount("hdfs://localhost:9000/input/infile/test_count_int1.txt");
        statisticsWordCount("D:\\opayProduct\\hadoop\\conf\\kill_bird.txt");
    }

    /**
     * 统计单词总个数ok
     */
    public static void statisticsWordCount(String filePath){
        SparkConf sparkConf = new SparkConf().setAppName("statisticsWordCount").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(filePath);
        JavaRDD<String> splitRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                //进行word拆分--拆成一个一个单词
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        //当，某已RDD会被反复操作的时候，就需要缓存起来
        splitRDD.cache();

        List<String> excludeList = Arrays.asList(":", ",", "，", "。", "《", "》", "？", "！"," ","；");
        //标点符号
        JavaRDD<String> markRDD = splitRDD.filter(s -> excludeList.contains(s));
        //除标点符号外的单词
        JavaRDD<String> wordRDD = splitRDD.filter(s -> !excludeList.contains(s));

        long markSum = markRDD.count();
        long wordSum = wordRDD.count();

        //打印具体信息
        List<String> collect = markRDD.collect();
        for(String m:collect){
            System.out.print(m);
        }
        System.out.println();

        //在真正执行action的时候，再释放缓存
        splitRDD.unpersist();

        System.out.println("markSum:"+markSum);
        System.out.println("wordSum:"+wordSum);

        ctx.stop();
    }

    /**
     * 内存中数据进行排序
     * ok
     */
    public static void sortOperate(){
        SparkConf sparkConf = new SparkConf().setAppName("base").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        //从内存集合中创建RDD
        JavaRDD<String> javaRDD = ctx.parallelize(Arrays.asList("111", "222", "333","888","777","222"));

        //String转换Long
        JavaRDD<Long> parseMapRDD = javaRDD.map(new Function<String, Long>() {
            @Override
            public Long call(String s) throws Exception {
                return Long.parseLong(s);
            }
        });


        //进行排序
        JavaRDD<Long> sortRDD = parseMapRDD.sortBy(new Function<Long, Long>() {

            @Override
            public Long call(Long s) throws Exception {
                return s;
            }
        }, false, 2);

        //聚合输出
        List<Long> collect = sortRDD.collect();
        for(Long d:collect){
            System.out.println(d);
        }

    }

    /**
     * 统计各个字出现个数
     * ok
     * @param args
     * @throws Exception
     */
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
         *
         *  reduceByKey需要进行shuffle操作
         *  以 Shuffle 为边界，reduceByKey 的计算被切割为两个执行阶段。约定俗成地，我们把 Shuffle 之前的 Stage 叫作 Map 阶段，
         *  而把 Shuffle 之后的 Stage 称作 Reduce 阶段。在 Map 阶段，每个 Executors 先把自己负责的数据分区做初步聚合（又叫 Map 端聚合、局部聚合）；
         *  在 Shuffle 环节，不同的单词被分发到不同节点的 Executors 中；最后的 Reduce 阶段，Executors 以单词为 Key 做第二次聚合（又叫全局聚合），
         *  从而完成统计计数的任务。
         *
         *  Stage会有多个task，在map阶段的每个task会生成中间文件，reduce阶段消费这些中间文件。
         *
         *
         **/
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

        // 交换key，再排序--元数据key-value进行交换
        JavaPairRDD<Integer, String> dataSwap = counts.mapToPair(tp -> tp.swap());
        //通过交换后的value-key通过value进行降序排序
        JavaPairRDD<Integer, String> dataSort = dataSwap.sortByKey(false);
        //排完序的元数据，再交换回来
        JavaPairRDD<String, Integer> resultSort = dataSort.mapToPair(tp -> tp.swap());

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
        List<Tuple2<String, Integer>> output = resultSort.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        ctx.stop();
    }

    /**
     * RDD 函数解释
     * filter：的时候会过滤掉那些返回只为false的数据
     * lines.collect();  List<String>
     * lines.union();    javaRDD<String>
     * lines.top(1);     List<String>
     * lines.count();    long
     * lines.countByValue();
     * lines.cache();   //暂时放在缓存中，一般用于哪些可能需要多次使用的RDD，据说这样会减少运行时间
     * counts.sortByKey();  通过key进行排序
     * collect：该方法用于将spark的RDD类型转化为我们熟知的java常见类型
     */
}
