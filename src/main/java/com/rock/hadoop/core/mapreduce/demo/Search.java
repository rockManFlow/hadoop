package com.rock.hadoop.core.mapreduce.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.StringTokenizer;

/**
 * @author caoqingyuan
 * @detail
 * @date 2018/9/20 20:00
 */
public class Search {
    private static final Logger logger= LoggerFactory.getLogger(Search.class);
    public static class Map extends Mapper<Object,Text,Text,Text>{
        private static final String word="月";
        private FileSplit fileSplit;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        }
        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
            fileSplit=(FileSplit)context.getInputSplit();
            String filename=fileSplit.getPath().getName().toString();
            StringTokenizer st=new StringTokenizer(value.toString(),"。");
            while (st.hasMoreTokens()){
                String line=st.nextToken().toString();
                if(line.indexOf(word)>=0){
                    context.write(new Text(filename),new Text(line));
                }
            }
            logger.info("map run");
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        @Override
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
            String lines = "";
            for(Text value:values){
                lines += value.toString()+"---|---";
            }
            context.write(key, new Text(lines));
            logger.info("reduce run");
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        logger.info("hadoop start...");
        Configuration conf=new Configuration();
        conf.set("mapred.job.tracker","192.168.171.129:50090");
        args=new String[]{"hdfs://192.168.171.129:9000/input/file/int2.txt","hdfs://192.168.171.129:9000/output/file3"};
//
        String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length != 2){
            logger.info("Usage search <int> <out>");
            System.exit(2);
        }
        //配置作业名
        Job job = Job.getInstance(conf);
        //配置作业各个类
        job.setJarByClass(Search.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path path = new Path(otherArgs[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
