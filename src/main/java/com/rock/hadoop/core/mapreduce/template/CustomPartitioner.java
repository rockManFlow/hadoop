package com.rock.hadoop.core.mapreduce.template;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Hadoop自带了一个默认的分区类HashPartitioner
 *
 * Partitioner组件可以让Map对Key进行分区，从而可以根据不同的key分发到不同的Reduce中去处理，其目的就是将key均匀分布在ReduceTask上
 */
public class CustomPartitioner extends Partitioner {
    @Override
    public int getPartition(Object o, Object o2, int i) {
        return 0;
    }
}
