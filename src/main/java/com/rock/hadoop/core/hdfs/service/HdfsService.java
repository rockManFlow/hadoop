package com.rock.hadoop.core.hdfs.service;

import java.io.IOException;

public interface HdfsService {
    /**
     * 加载本地文件到hdfs中(循环写数据到远端，防止文件过大导致内存溢出)
     * @param filePath
     * @return
     */
    boolean loadFileToHdfs(String filePath,String remotePath) throws IOException;

    /**
     * 往指定路径下追加数据（路径不存在会创建再追加）
     * @param remotePath
     * @param data
     * @return
     */
    boolean appendToFile(String remotePath,byte[] data);

    /**
     * 从远端下载文件（如果是文件夹会下载文件夹中的所有文件到本地）
     * @param remotePath 远端路径（文件路径或者文件夹路径）
     * @param localPath 本地路径（不包含文件名）
     * @return
     * @throws IOException
     */
    boolean downloadFile(String remotePath,String localPath) throws IOException;

    /**
     * 删除远端文件
     * @param remotePath
     * @return
     * @throws IOException
     */
    boolean deleteFile(String remotePath) throws IOException;

    /**
     * 创建文件夹
     * @param remotePath
     * @return
     * @throws IOException
     */
    boolean mkdir(String remotePath) throws IOException;
}
