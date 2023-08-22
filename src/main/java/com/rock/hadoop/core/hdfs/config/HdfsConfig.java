package com.rock.hadoop.core.hdfs.config;

import org.apache.hadoop.conf.Configuration;

public class HdfsConfig {
    public static Configuration getConfiguration(){
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
        return conf;
    }
}
