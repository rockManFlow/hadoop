package com.rock.hadoop.core.mapreduce.demo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author caoqingyuan
 * @detail
 * @date 2018/9/25 11:30
 */
public class MyJob {
    public static class MapClass extends Mapper<Text,Text,Text,Text>{
        @Override
        public void map(Text key,Text value,Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }
}
