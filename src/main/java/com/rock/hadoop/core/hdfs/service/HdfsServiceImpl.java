package com.rock.hadoop.core.hdfs.service;

import com.rock.hadoop.core.hdfs.config.HdfsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

/**
 * 进行hdfs的基本操作
 */
@Slf4j
public class HdfsServiceImpl implements HdfsService{
    @Override
    public boolean loadFileToHdfs(String filePath,String remotePath) throws IOException {
        Configuration conf = HdfsConfig.getConfiguration();
        Path dstPath = new Path(remotePath);
        FSDataOutputStream outputStream =null;
        RandomAccessFile randomAccessFile =null;
        try {
            FileSystem fs = dstPath.getFileSystem(conf);
            outputStream = fs.create(dstPath);

            randomAccessFile = new RandomAccessFile(filePath, "r");
            byte[] data = new byte[1024];
            int i;
            while (true) {
                i = randomAccessFile.read(data);
                if (i == -1) {
                    break;
                }
                outputStream.write(data);
                outputStream.flush();
                data = new byte[1024];
            }

            log.info("create file success!");
            return true;
        }catch (IOException e){
            log.error("loadFileToHdfs error",e);
        }finally {
            outputStream.close();
            randomAccessFile.close();
        }

        return false;
    }

    @Override
    public boolean appendToFile(String remotePath, byte[] data) {
        FSDataOutputStream out=null;
        try {
            FileSystem fileSystem=FileSystem.get(HdfsConfig.getConfiguration());
            Path path=new Path(remotePath);
            if(!fileSystem.exists(path)){
                //创建文件数据的输出流
                out=fileSystem.create(new Path(remotePath));
            }else{
                //往文件中追加数据
                out=fileSystem.append(path);
            }
            //通过输出流往hdfs中写入数据
            out.write(data,0,data.length);
            out.flush();
            return true;
        } catch (IOException e) {
            log.error("appendToFile error",e);
        }finally{
            //关闭输出流
            if(out!=null){
                try {
                    out.close();
                } catch (IOException e) {
                    log.error("appendToFile IO error",e);
                }
            }
        }
        return false;
    }

    @Override
    public boolean downloadFile(String remotePath, String localPath) throws IOException {
        Path path = new Path(remotePath);
        FileSystem hdfs = path.getFileSystem(HdfsConfig.getConfiguration());

        File rootfile = new File(localPath);
        if (!rootfile.exists()) {
            rootfile.mkdirs();
        }

        if (hdfs.isFile(path)) {
            FSDataInputStream in = null;
            FileOutputStream out = null;
            try {
                in = hdfs.open(path);
                File srcfile = new File(rootfile, path.getName());
                if (!srcfile.exists()) {
                    srcfile.createNewFile();
                }
                out = new FileOutputStream(srcfile);
                IOUtils.copyBytes(in, out, 4096, false);
                return true;
            } finally {
                IOUtils.closeStream(in);
                IOUtils.closeStream(out);
            }
        } else if (hdfs.isDirectory(path)) {
            File dstDir = new File(localPath);
            if (!dstDir.exists()) {
                dstDir.mkdirs();
            }
            //在本地目录上加一层子目录
            String filePath = path.toString();//目录
            String subPath[] = filePath.split("/");
            String newdstPath = localPath + subPath[subPath.length - 1] + "/";
            log.info("local new file path:{}",newdstPath);
            if (hdfs.exists(path) && hdfs.isDirectory(path)) {
                FileStatus[] srcFileStatus = hdfs.listStatus(path);
                if (srcFileStatus != null) {
                    for (FileStatus status : hdfs.listStatus(path)) {
                        //下载子目录下文件
                        downloadFile(status.getPath().toString(),newdstPath);
                    }
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean deleteFile(String remotePath) throws IOException {
        Path path = new Path(remotePath);
        FileSystem fs = path.getFileSystem(HdfsConfig.getConfiguration());

        boolean isok = fs.deleteOnExit(path);
        if (isok) {
            return true;
        }
        return false;
    }

    @Override
    public boolean mkdir(String remotePath) throws IOException {
        Path srcPath = new Path(remotePath);
        FileSystem fs = srcPath.getFileSystem(HdfsConfig.getConfiguration());

        boolean isok = fs.mkdirs(srcPath);
        if (isok) {
            return true;
        }
        return false;
    }
}
