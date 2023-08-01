package com.rock.hadoop.core.hdfs.service;

import com.rock.hadoop.core.hdfs.util.HdfsUtil;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 进行hdfs的基本操作
 */
public class HdfsService {
    public static void main(String[] args) throws Exception {
//        write();
        read();
    }

    public static void read() throws Exception {
        String hdfsUrl="hdfs://127.0.0.1:9000";
        HdfsUtil hdfsUtil=new HdfsUtil(hdfsUrl);
        //输出到控制台-OK
        hdfsUtil.readFile("/input/local_int4.txt");

        //ok
//        hdfsUtil.downloadFileByte("local_int3.txt");

//        String[] fileList = hdfsUtil.getFileList("local_int3.txt");
//        for(String url:fileList){
//            System.out.println(url);
//        }
    }

    public static void write() throws IOException {
        String hdfsUrl="hdfs://127.0.0.1:9000";
        HdfsUtil hdfsUtil=new HdfsUtil(hdfsUrl);
        RandomAccessFile randomAccessFile=new RandomAccessFile("conf/int4.txt","rw");
        byte[] data=new byte[1024];
        int i;
        while (true){
            i=randomAccessFile.read(data);
            if(i==-1){
                break;
            }
            hdfsUtil.createFile("/input/local_int4.txt",data);
            data=new byte[1024];
        }
    }


}
