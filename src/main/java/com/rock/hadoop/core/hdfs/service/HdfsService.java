package com.rock.hadoop.core.hdfs.service;

import com.rock.hadoop.core.hdfs.util.HdfsUtil;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 进行hdfs的基本操作
 */
public class HdfsService {
    public static void main(String[] args) throws IOException {
        String hdfsUrl="hdfs://127.0.0.1:19000";
        HdfsUtil hdfsUtil=new HdfsUtil(hdfsUrl);
        RandomAccessFile randomAccessFile=new RandomAccessFile("conf/int3.txt","rw");
//        StringBuilder sb=new StringBuilder();
        byte[] data=new byte[1024];
        int i;
        while (true){
            i=randomAccessFile.read(data);
            if(i==-1){
                break;
            }
            hdfsUtil.createFile("local_int3.txt",data);
            data=new byte[1024];
        }

    }


}
