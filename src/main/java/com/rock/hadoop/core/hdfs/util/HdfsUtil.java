package com.rock.hadoop.core.hdfs.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author caoqingyuan
 * @detail
 * @date 2018/9/27 16:20
 */
@Slf4j
public class HdfsUtil {
    private FileSystem fileSystem;
    private Configuration conf;
    public HdfsUtil(){
        String hdfsUrl="hdfs://127.0.0.1:19000";
        try {
            fileSystem=FileSystem.get(URI.create(hdfsUrl),new Configuration());
        } catch (IOException e) {
            log.error("get fileSystem error",e);
        }

        conf = new Configuration();
        // 这里指定使用的是HDFS文件系统
        conf.set("fs.defaultFS", hdfsUrl);
    }

    /**
     * 将本地的文件上传至HDFS
     * @param localPath
     * @param remotePath
     */
    public void put(String localPath,String remotePath){
        try {
            fileSystem.copyFromLocalFile(new Path(localPath),new Path(remotePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * 往hdfs中写数据
     */
    public void writeToHdfs(String filename,String text){
        FSDataOutputStream out=null;
        String charset="UTF-8";
        try {
            Path path=new Path(filename);
            if(!fileSystem.exists(path)){
                //创建文件数据的输出流
                out=fileSystem.create(new Path(filename));
                //通过输出流往hdfs中写入数据
                out.write(text.getBytes(charset),0,text.getBytes(charset).length);
                out.write("\n".getBytes(charset),0,"\n".getBytes(charset).length);
                out.flush();
            }else{
                //往文件中追加数据
                out=fileSystem.append(path);
                out.write(text.getBytes(charset),0,text.getBytes(charset).length);
                out.write("\n".getBytes(charset),0,"\n".getBytes(charset).length);
                out.flush();
            }
        } catch (IOException e) {
            log.error("往HDFS中放数据异常",e);
        }finally{
            //关闭输出流
            if(out!=null){
                try {
                    out.close();
                } catch (IOException e) {
                    log.error("往HDFS中放数据关闭输出流异常",e);
                }
            }
        }
    }

    /*
     * 从hdfs中读取数据
     */
    public void readFromHdfs(String fileName){
        Path filePath=new Path(fileName);
        try {
            if(fileSystem.exists(filePath)){
                String charset="GBK";
                //打开文件数据输入流
                FSDataInputStream fsDataInputStream=fileSystem.open(filePath);
                //创建文件输入
                InputStreamReader inputStreamReader=new InputStreamReader(fsDataInputStream,charset);
                String line=null;
                //把数据读入到缓冲区中
                BufferedReader reader=null;
                reader=new BufferedReader(inputStreamReader);
                //从缓冲区中读取数据
                while((line=reader.readLine())!=null){
                    System.out.println(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取hdfs路径下的文件列表
     *
     * @param srcpath
     * @return
     */
    public String[] getFileList(String srcpath) {
        try {
            Path path = new Path(srcpath);
            FileSystem fs = path.getFileSystem(conf);
            List<String> files = new ArrayList<String>();
            if (fs.exists(path) && fs.isDirectory(path)) {
                for (FileStatus status : fs.listStatus(path)) {
                    files.add(status.getPath().toString());
                }
            }
            return files.toArray(new String[]{});
        } catch (IOException e) {
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * 给定文件名和文件内容，创建hdfs文件
     *
     * @param dst
     * @param contents
     * @throws IOException
     */
    public void createFile(String dst, byte[] contents)
            throws IOException {
        Path dstPath = new Path(dst);
        FileSystem fs = dstPath.getFileSystem(conf);

        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(contents);
        outputStream.close();
        System.out.println("create file " + dst + " success!");
    }

    /**
     * 删除hdfs文件
     *
     * @param filePath
     * @throws IOException
     */
    public void delete(String filePath) throws IOException {
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(conf);

        boolean isok = fs.deleteOnExit(path);
        if (isok) {
            System.out.println("delete file " + filePath + " success!");
        } else {
            System.out.println("delete file " + filePath + " failure");
        }
    }

    /**
     * 创建hdfs目录
     *
     * @param path
     * @throws IOException
     */
    public void mkdir(String path) throws IOException {
        Path srcPath = new Path(path);
        FileSystem fs = srcPath.getFileSystem(conf);

        boolean isok = fs.mkdirs(srcPath);
        if (isok) {
            System.out.println("create dir ok!");
        } else {
            System.out.println("create dir failure");
        }
    }

    /**
     * 读取hdfs文件内容，并在控制台打印出来
     *
     * @param filePath
     * @throws IOException
     */
    public void readFile(String filePath) throws IOException {
        Path srcPath = new Path(filePath);
        FileSystem fs = null;
        URI uri;
        try {
            uri = new URI(filePath);
            fs = FileSystem.get(uri, conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        InputStream in = null;
        try {
            in = fs.open(srcPath);
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * 下载hdfs文件到本地目录
     *
     * @param dstPath
     * @param srcPath
     * @throws Exception
     */
    public void downloadFile(String dstPath, String srcPath) throws Exception {
        Path path = new Path(srcPath);
        FileSystem hdfs = path.getFileSystem(conf);

        File rootfile = new File(dstPath);
        if (!rootfile.exists()) {
            rootfile.mkdirs();
        }

        if (hdfs.isFile(path)) {
            //只下载非txt文件
            String fileName = path.getName();
            if (!fileName.toLowerCase().endsWith("txt")) {
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
                } finally {
                    IOUtils.closeStream(in);
                    IOUtils.closeStream(out);
                }
                //下载完后，在hdfs上将原文件删除
                this.delete(path.toString());
            }
        } else if (hdfs.isDirectory(path)) {
            File dstDir = new File(dstPath);
            if (!dstDir.exists()) {
                dstDir.mkdirs();
            }
            //在本地目录上加一层子目录
            String filePath = path.toString();//目录
            String subPath[] = filePath.split("/");
            String newdstPath = dstPath + subPath[subPath.length - 1] + "/";
            System.out.println("newdstPath=======" + newdstPath);
            if (hdfs.exists(path) && hdfs.isDirectory(path)) {
                FileStatus[] srcFileStatus = hdfs.listStatus(path);
                if (srcFileStatus != null) {
                    for (FileStatus status : hdfs.listStatus(path)) {
                        //下载子目录下文件
                        downloadFile(newdstPath, status.getPath().toString());
                    }
                }
            }
        }
    }

    /**
     * 下载hdfs文件内容，保存到内存对象中
     *
     * @param srcPath
     * @throws Exception
     */
    public void downloadFileByte(String srcPath) throws Exception {
        Path path = new Path(srcPath);
        if (fileSystem.exists(path)) {
            if (fileSystem.isFile(path)) {
                //如果是文件
                FSDataInputStream in = null;
                FileOutputStream out = null;
                try {
                    in = fileSystem.open(new Path(srcPath));
                    byte[] t = new byte[in.available()];
                    in.read(t);
                    System.out.println("data:"+new String(t));
                } finally {
                    IOUtils.closeStream(in);
                    IOUtils.closeStream(out);
                }
            } else {
                //如果是目录
                FileStatus[] srcFileStatus = fileSystem.listStatus(new Path(srcPath));
                for (int i = 0; i < srcFileStatus.length; i++) {
                    String srcFile = srcFileStatus[i].getPath().toString();
                    downloadFileByte(srcFile);
                }
            }
        }
    }

    /**
     * 将本地目录或文件上传的hdfs
     *
     * @param localSrc
     * @param dst
     * @throws Exception
     */
    public void uploadFile(String localSrc, String dst) throws Exception {
        File srcFile = new File(localSrc);
        if (srcFile.isDirectory()) {
            copyDirectory(localSrc, dst);
        } else {
            copyFile(localSrc, dst);
        }
    }

    /**
     * 拷贝本地文件hdfs目录下
     *
     * @param localSrc
     * @param dst
     * @return
     * @throws Exception
     */
    private boolean copyFile(String localSrc, String dst) throws Exception {
        File file = new File(localSrc);
        dst = dst + file.getName();
        Path path = new Path(dst);
        FileSystem fs = path.getFileSystem(conf);//FileSystem.get(conf);
        fs.exists(path);
        InputStream in = new BufferedInputStream(new FileInputStream(file));
        OutputStream out = fs.create(new Path(dst));
        IOUtils.copyBytes(in, out, 4096, true);
        in.close();
        return true;
    }

    /**
     * 拷贝本地目录到hdfs
     * @param src
     * @param dst
     * @return
     * @throws Exception
     */
    private boolean copyDirectory(String src, String dst) throws Exception {
        Path path = new Path(dst);
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
        File file = new File(src);

        File[] files = file.listFiles();
        for (int i = 0; i < files.length; i++) {
            File f = files[i];
            if (f.isDirectory()) {
                String fname = f.getName();
                if (dst.endsWith("/")) {
                    copyDirectory(f.getPath(), dst + fname + "/");
                } else {
                    copyDirectory(f.getPath(), dst + "/" + fname + "/");
                }
            } else {
                copyFile(f.getPath(), dst);
            }
        }
        return true;
    }

    /**
     * ok 没问题
     * @throws IOException
     */
    public static void writeContext() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://192.168.234.129:9000");
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(conf);
        byte[] buff = "this is my first hadoop test".getBytes(); // 要写入的内容
        String filename = "/input/infile/test_file2.txt"; //要写入的文件名
        FSDataOutputStream os = fs.create(new Path(filename));
        os.write(buff,0,buff.length);
        System.out.println("Create:"+ filename);
        os.close();
        fs.close();
    }
}
