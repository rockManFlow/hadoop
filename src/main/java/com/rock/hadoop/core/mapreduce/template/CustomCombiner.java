package com.rock.hadoop.core.mapreduce.template;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 在Map阶段输出可能会产生大量相同的数据，例如<hello，1>、<hello，1>……，势必会降低Reduce聚合阶段的执行效率。
 * Combiner组件的作用就是对Map阶段的输出的重复数据先做一次合并计算，
 * 然后把新的（key，value）作为Reduce阶段的输入。
 *
 * 另外还需要在主运行类中为Job设置Combiner组件即可，具体代码如下：
 * wcjob.setCombinerClass(WordCountCombiner.class);
 *
 * 执行MapReduce程序，添加与不添加Combiner结果是一致的。
 * 通俗的讲，无论调用多少次Combiner，Reduce的输出结果都是一样的，因为Combiner组件不允许改变业务逻辑。
 */
public class CustomCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    }
}
