package com.rock.hadoop.core.hdfs;

import com.rock.hadoop.core.hdfs.util.HdfsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.RandomAccessFile;

public class HdfsMain {
    public static void main(String[] args) throws Exception {
//        writeCycle();
//        read("/input/infile/test_count_int1.txt");
        //查询通过MapReduce计算的结果文件信息
//        read("/input/outfile/test_count_int1_result.txt");

        read("/input/infile/test_count_int1.txt");

//        write();
//        String hdfsUrl="hdfs://127.0.0.1:9000";
//        HdfsUtil hdfsUtil=new HdfsUtil(hdfsUrl);
//        hdfsUtil.downloadFileByte("/input/outfile/test_count_int2");
    }

    /**
     * hdfs dfsadmin -safemode leave  离开安全模式
     * ok
     * @throws IOException
     */
    public static void writeCycle() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
        Path dstPath = new Path("/input/infile/test_count_int1.txt");
        FileSystem fs = dstPath.getFileSystem(conf);
        FSDataOutputStream outputStream = fs.create(dstPath);


        RandomAccessFile randomAccessFile=new RandomAccessFile("conf/int1.txt","r");
        byte[] data=new byte[32];
        int i;
        while (true){
            i=randomAccessFile.read(data);
            if(i==-1){
                break;
            }
            outputStream.write(data,0,i);
            outputStream.flush();
        }

        outputStream.close();
        System.out.println("create file success!");
    }

    public static void loadFileList(String filePath){
        String hdfsUrl="hdfs://127.0.0.1:9000";
        HdfsUtil hdfsUtil=new HdfsUtil(hdfsUrl);
        //输出到控制台-OK
        String[] fileList = hdfsUtil.getFileList(filePath);
        for(String file:fileList){
            System.out.println(file);
        }
    }
    public static void read(String filePath) throws Exception {
        String hdfsUrl="hdfs://127.0.0.1:9000";
        HdfsUtil hdfsUtil=new HdfsUtil(hdfsUrl);
        //输出到控制台-OK
        hdfsUtil.readFile(filePath);

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
        RandomAccessFile randomAccessFile=new RandomAccessFile("conf/int1.txt","r");
        byte[] data=new byte[1024];
        int i;
        while (true){
            i=randomAccessFile.read(data);
            if(i==-1){
                break;
            }
            hdfsUtil.createFile("/input/infile/test_count_int1.txt",data);
            data=new byte[1024];
        }
    }
}
