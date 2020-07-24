package com.rock.hadoop.core.mapreduce.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * @author caoqingyuan
 * @detail
 * @date 2018/10/8 15:31
 */
public class BaseLearning {
    public static void main(String[] args) throws IOException {

        //从HDFS中读取其中的文件，并通过控制台展示出来
        /**
         * Configuration封装了客户端或服务器的配置
         * 其中包括：通过设置配置文件读取类路径来实现(core-site.xml)中指定HDFS的存储路径
         */
        Configuration conf=new Configuration();
        /**
         * FileSystem是一个通用的文件系统api，获取其实例有两种静态方法
         * get(configuration)+get(uri,configuration)
         *
         * 还有往文件中写入数据，create方法
         *
         * 还提供了创建目录的方法mkdirs()
         */
        FileSystem fs = FileSystem.get(URI.create(args[0]),conf);
        InputStream in=null;
        try {
            //获取文件的输入流，默认是4KB
            in = fs.open(new Path(args[0]));
            IOUtils.copyBytes(in, System.out, 4096, false);
        }finally {
            IOUtils.closeStream(in);
        }
    }
    /**
     * @description 将本地文件上传到Hadoop文件系统
     * @author caoqingyuan
     * @param
     * @return void
     * @date 16:15 2018/10/8
     **/
    public void writeFileTohdfs(String[] args) throws IOException {
        String localSrc=args[0];
        String target=args[1];
        InputStream in=new BufferedInputStream(new FileInputStream(localSrc));
        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(URI.create(target),conf);
        OutputStream out=fs.create(new Path(target), new Progressable() {
            @Override
            public void progress() {
                System.out.println(".");
            }
        });

        IOUtils.copyBytes(in,out,4096,true);
    }
}
