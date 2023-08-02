package com.rock.hadoop.core.mapreduce.template;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomMap extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    public void setup(Context context)  {
        //1
    }

    @Override
    public void map(LongWritable key, Text value, Context context) {
        //2
    }

    @Override
    public void cleanup(Context context) {
        //3
    }
}
