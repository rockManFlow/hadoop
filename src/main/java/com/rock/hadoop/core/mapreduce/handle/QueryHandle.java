package com.rock.hadoop.core.mapreduce.handle;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author rock
 * @detail 统计文档中指定汉字出现的个数
 * @date 2020/8/11 14:51
 */
@Slf4j
public class QueryHandle {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置文件
        Configuration conf = new Configuration();
//        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
//        conf.set("fs.defaultFS", "hdfs://192.168.234.129:9000");
//        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
//        conf.set("mapred.job.tracker","192.168.234.129:50090");
//        conf.set("hadoop.home.dir","192.168.234.129:9000");
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

        //指定处理结果的输出数据存放路径---文件夹路径
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.234.129:9000/input/outfile"));

        //将job提交给集群运行
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
