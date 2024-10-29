package com.rock.hadoop.core.spark.demo;

import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * zip暴力破解解压
 * 1、spark中公共参数如何处理？
 * 2、编写的一个处理程序，会如何被处理？
 * 3、一个spark程序是如何在集群中运行的？
 */
public class ZipCrackerSpark {
    private static AtomicBoolean finish=new AtomicBoolean(false);
    private static String unZipDirectoryPath;
    private static final Object lock=new Object();

    public static void main(String[] args) throws ZipException {
        String zipFilePath="D:/myProjects/hadoop/conf/zip/test02.zip";
        String passwordFile="D:/myProjects/hadoop/conf/zip/password_list.txt";
        crackerZip(zipFilePath,passwordFile);
    }

    public static void crackerZip(String zipFilePath,String passFilePath) throws ZipException {
        SparkConf sparkConf = new SparkConf().setAppName("zipCracker").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        //读取整个文件信息
        JavaRDD<String> lines = ctx.textFile(passFilePath);
        //打印具体信息--这个操作是把所有集群上节点上的结果进行聚合到一个节点上，可能会占用该节点很大内存导致内存溢出，并且是串行的
//        List<String> collect = lines.collect();
//        for(String m:collect){
//            unzip(zipFilePath,m);
//        }

        //这种可以并行执行，但操作需要动作才会执行
        JavaRDD<String> map = lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                unzip(zipFilePath,s);
                return s;
            }
        });
        map.count();//动作计数


        ctx.stop();
    }

    public static void unzip(String zipFilePath, String passwd) throws ZipException {
        if(finish.get()){
            return;
        }
        System.out.println("Pass:"+passwd);
        try(ZipFile zFile = new ZipFile(zipFilePath)){// 首先创建ZipFile指向磁盘上的.zip文件
            if (!zFile.isValidZipFile()) {   // 验证.zip文件是否合法，包括文件是否存在、是否为zip文件、是否被损坏等
                throw new ZipException("压缩文件不合法,可能被损坏.");
            }
            if (zFile.isEncrypted()) {
                zFile.setPassword(passwd.toCharArray());  // 设置密码
            }
            createUnZipDirectoryPath(zipFilePath);
            zFile.extractAll(unZipDirectoryPath);      // 将文件抽出到解压目录(解压)
            finish.compareAndSet(false,true);
            System.out.println("正确密码："+passwd);
        } catch (IOException e) {
//            e.printStackTrace();
        }

    }

    private static void createUnZipDirectoryPath(String zipFilePath){
        if(StringUtils.isEmpty(zipFilePath)){
            System.err.println("null zipFilePath");
            throw new RuntimeException("null zipFilePath");
        }
        if(!StringUtils.isEmpty(unZipDirectoryPath)){
            return;
        }

        synchronized (lock){
            String path=zipFilePath.substring(0,zipFilePath.lastIndexOf("/"));
            String zipAllFileName=zipFilePath.substring(zipFilePath.lastIndexOf("/"));
            String zipFileName=zipAllFileName.substring(0,zipAllFileName.lastIndexOf("."));
            unZipDirectoryPath=new StringBuilder(path).append("/").append(zipFileName).append("/").toString();

            // 创建File对象
            File directory = new File(unZipDirectoryPath);

            // 如果文件夹不存在，则创建文件夹
            if (!directory.exists()) {
                boolean result = directory.mkdirs();
                if (result) {
                    System.out.println("文件夹创建成功：" + unZipDirectoryPath);
                } else {
                    System.out.println("文件夹创建失败：" + unZipDirectoryPath);
                }
            }
            System.out.println("unZipDirectoryPath:"+unZipDirectoryPath);
        }
    }
}
