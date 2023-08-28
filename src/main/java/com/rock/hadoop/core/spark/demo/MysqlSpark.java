package com.rock.hadoop.core.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * sql表达式如何来写？？
 */
public class MysqlSpark {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("SparkMysql").setMaster("local[5]"));
        SQLContext sqlContext = new SQLContext(sparkContext);
        //读取mysql数据
        readMySQL(sqlContext);
        //写数据
//        writeDb(sqlContext,sparkContext);

        //停止SparkContext
        sparkContext.stop();
    }

    private static void readMySQL(SQLContext sqlContext){
        //查找的表名
        String table = "card_config_dict";
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = buildDbProperties();

        //SparkJdbc读取Postgresql的products表内容
        System.out.println("读取"+table+"表内容");
        // 读取表中所有数据
        Dataset<Row> select = sqlContext.read().jdbc(connectionProperties.getProperty("url"), table, connectionProperties).select("*");
        select.orderBy(select.col("id").desc());
        //显示数据--显示行数
        select.show(10);
    }

    private static void writeDb(SQLContext sqlContext,JavaSparkContext sparkContext){
        //写入的数据内容--从集合中来获取rdd
        JavaRDD<String> personData = sparkContext.parallelize(Arrays.asList("regionA 210 sparkA 566 1 NORMAL","regionB 220 sparkB 566 1 NORMAL"));
        //数据库内容
        Properties connectionProperties = buildDbProperties();
        String table="card_config_dict";
        /**
         * 第一步：在RDD的基础上创建类型为Row的RDD
         */
        //将RDD变成以Row为类型的RDD。Row可以简单理解为Table的一行数据
        JavaRDD<Row> lineDataRDD = personData.map(new Function<String,Row>(){
            public Row call(String line) throws Exception {
                String[] splited = line.split(" ");
                //构建行数据
                return RowFactory.create(splited[0],splited[1],splited[2],splited[3],Integer.valueOf(splited[4]),splited[5]);
            }
        });

        /**
         * 第二步：动态构造DataFrame的元数据。
         */
        List structFields = new ArrayList();
        structFields.add(DataTypes.createStructField("type",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("code",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("parent_code",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("sort",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("status",DataTypes.StringType,true));

        //构建StructType，用于最后DataFrame元数据的描述--数据结构体类型
        StructType tableStructType = DataTypes.createStructType(structFields);

        /**
         * 第三步：基于已有的元数据以及RDD<Row>来构造Dataset
         */
        Dataset<Row> dataset = sqlContext.createDataFrame(lineDataRDD, tableStructType);

        /**
         * 第四步：将数据写入到表中
         */
        dataset.write().mode(SaveMode.Append).jdbc(connectionProperties.getProperty("url"),table,connectionProperties);
    }

    private static Properties buildDbProperties(){
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","Admin172.16.22.231");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");
        connectionProperties.put("url","jdbc:mysql://182.160.17.58:3306/opay_card_read");
        return connectionProperties;
    }
}
