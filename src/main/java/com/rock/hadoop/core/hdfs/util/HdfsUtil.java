package com.rock.hadoop.core.hdfs.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * @author caoqingyuan
 * @detail
 * @date 2018/9/27 16:20
 */
public class HdfsUtil {
    private static final Logger logger= LoggerFactory.getLogger(HdfsUtil.class);
    /*
     * 往hdfs中写数据
     */
    public static void writeToHdfs(String filename,String text){
        //应该默认是从本机安装的Hadoop中加载对应的信息
        Configuration configuration=new Configuration();
        FSDataOutputStream out=null;
        String charset="UTF-8";
        try {
            FileSystem fSystem=FileSystem.get(URI.create(filename),configuration);
            Path path=new Path(filename);
            if(!fSystem.exists(path)){
                //创建文件数据的输出流
                out=fSystem.create(new Path(filename));
                //通过输出流往hdfs中写入数据
                out.write(text.getBytes(charset),0,text.getBytes(charset).length);
                out.write("\n".getBytes(charset),0,"\n".getBytes(charset).length);
                out.flush();
            }else{
                //往文件中追加数据
                out=fSystem.append(path);
                out.write(text.getBytes(charset),0,text.getBytes(charset).length);
                out.write("\n".getBytes(charset),0,"\n".getBytes(charset).length);
                out.flush();
            }
        } catch (IOException e) {
            logger.error("往HDFS中放数据异常",e);
        }finally{
            //关闭输出流
            if(out!=null){
                try {
                    out.close();
                } catch (IOException e) {
                    logger.error("往HDFS中放数据关闭输出流异常",e);
                }
            }
        }
    }

    /*
     * 从hdfs中读取数据
     */
    public static void readFromHdfs(String fileName){
        Configuration conf=new Configuration();
        Path filePath=new Path(fileName);
        try {
            FileSystem fs=FileSystem.get(URI.create(fileName),conf);
            if(fs.exists(filePath)){
                String charset="UTF-8";
                //打开文件数据输入流
                FSDataInputStream fsDataInputStream=fs.open(filePath);
                //创建文件输入
                InputStreamReader inputStreamReader=new InputStreamReader(fsDataInputStream,charset);
                String line=null;
                //把数据读入到缓冲区中
                BufferedReader reader=null;
                reader=new BufferedReader(inputStreamReader);
                //从缓冲区中读取数据
                while((line=reader.readLine())!=null){
                    logger.info("line="+line);
                }
            }
        } catch (IOException e) {
            logger.error("从HDFS中取数据异常",e);
        }
    }

    public static void main(String[] args){
        //String filename="hdfs://119.29.167.178:9000/test";
        String filename="/test/file1";
        String text="我们很6666";
        writeToHdfs(filename,text);
        writeToHdfs(filename, "5555555");
        readFromHdfs(filename);
    }
}
