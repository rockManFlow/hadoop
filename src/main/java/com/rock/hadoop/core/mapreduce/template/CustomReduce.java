package com.rock.hadoop.core.mapreduce.template;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 与map执行框架是一样的。调用run-(setup\reduce\cleanup)
 */
public class CustomReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
    /**
     * 在reduce执行之前，会有一次合并，相同key的value会放到reduce入参的集合中
     * 第一个参数是key，第二个参数是集合。
     * 框架在map处理完成之后，将所有key-value对缓存起来，进行分组，然后传递一个组<key,valus{}>，调用一次reduce方法
     * <hello,{1,1,1,1,1,1.....}>
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    }
}
