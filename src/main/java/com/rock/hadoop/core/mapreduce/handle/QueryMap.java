package com.rock.hadoop.core.mapreduce.handle;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author rock
 * @detail
 * @date 2020/9/7 16:16
 */
@Slf4j
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
        //拆分单词
        String lineContext = value.toString();
        //以空格进行拆分
        StringTokenizer stringTokenizer = new StringTokenizer(lineContext, "\"");
        //遍历这个单词数组，输出为key-value的格式，将单词发送给reduce
        while (stringTokenizer.hasMoreTokens()) {
            String word = stringTokenizer.nextToken();
            if(":".equalsIgnoreCase(word)||",".equalsIgnoreCase(word)||"{".equalsIgnoreCase(word)||"}".equalsIgnoreCase(word)){
                System.out.println("排除不需要的字符");
                continue;
            }
            context.write(new Text(word), new LongWritable(1));
        }
    }
}
