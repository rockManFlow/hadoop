package com.rock.hadoop.core.mapreduce.handle;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author rock
 * @detail 统计文档中指定汉字出现的个数
 * @date 2020/8/11 14:51
 */
@Slf4j
public class QueryHandle {
    public class QueryMap extends Mapper<LongWritable, Text, Text, LongWritable> {
        /**
         * 4个泛型中，前两个是指定mapper输入数据的类型，KEYIN是输入的key的类型，VALUEIN是输入的value的值
         * KEYOUT是输入的key的类型，VALUEOUT是输入的value的值
         * 2：map和reduce的数据输入和输出都是以key-value的形式封装的。
         * 3：默认情况下，框架传递给我们的mapper的输入数据中，key是要处理的文本中一行的起始偏移量，这一行的内容作为value
         * 4：key-value数据是在网络中进行传递，节点和节点之间互相传递，在网络之间传输就需要序列化，但是jdk自己的序列化很冗余
         * 所以使用hadoop自己封装的数据类型，而不要使用jdk自己封装的数据类型；
         * Long--->LongWritable
         * String--->Text
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            log.info("inputSplit.getLocations():{}", inputSplit.getLocations());

            //拆分单词
            String lineContext = value.toString();
            //以空格进行拆分
            StringTokenizer stringTokenizer = new StringTokenizer(lineContext, " ");
            //遍历这个单词数组，输出为key-value的格式，将单词发送给reduce
            while (stringTokenizer.hasMoreTokens()) {
                String word = stringTokenizer.nextToken();
                context.write(new Text(word), new LongWritable(1));
            }
        }
    }

    /**
     * 在reduce执行之前，会有一次合并，相同key的value会放到reduce入参的集合中
     * 第一个参数是key，第二个参数是集合。
     * 框架在map处理完成之后，将所有key-value对缓存起来，进行分组，然后传递一个组<key,valus{}>，调用一次reduce方法
     * <hello,{1,1,1,1,1,1.....}>
     */
    public class QueryReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            Long count = 0L;
            //遍历该相同key出现的总次数
            for (LongWritable num : values) {
                count += num.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置文件
        Configuration conf = new Configuration();
        //获取一个作业
        Job job = Job.getInstance(conf);

        //设置整个job所用的那些类在哪个jar包
        job.setJarByClass(QueryHandle.class);

        //本job使用的mapper和reducer的类
        job.setMapperClass(QueryMap.class);
        job.setReducerClass(QueryReduce.class);

        //指定reduce的输出数据key-value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        //指定mapper的输出数据key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //指定要处理的输入数据存放路径
        FileInputFormat.addInputPath(job, new Path("hdfs://192.168.234.129:9000/input/infile/test_file.txt"));

        //指定处理结果的输出数据存放路径
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.234.129:9000/input/outfile/count.txt"));

        //将job提交给集群运行
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
