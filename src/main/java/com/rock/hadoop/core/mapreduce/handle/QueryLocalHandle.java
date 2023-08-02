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
 * 统计文档中指定汉字出现的个数--在本地也是可以的（没hdfs服务），文件仅是保存在本地--都是文件系统
 */
@Slf4j
public class QueryLocalHandle {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置文件
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        //获取一个作业
        Job job = Job.getInstance(conf);

        //设置整个job所用的那些类在哪个jar包
        job.setJarByClass(QueryLocalHandle.class);

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
        FileInputFormat.addInputPath(job, new Path("D:\\opayProduct\\hadoop\\conf\\int1.txt"));

        //指定处理结果的输出数据存放路径---文件目录(ps：跟引用包的版本有关系)
        FileOutputFormat.setOutputPath(job, new Path("D:\\opayProduct\\hadoop\\conf/out"));
        //将job提交给集群运行
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
