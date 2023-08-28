package com.rock.hadoop.core.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Properties;

public class MysqlSpark {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("SparkMysql").setMaster("local[5]"));
        SQLContext sqlContext = new SQLContext(sparkContext);
        //读取mysql数据
        readMySQL(sqlContext);

        //停止SparkContext
        sparkContext.stop();
    }
    private static void readMySQL(SQLContext sqlContext){
        String url = "jdbc:mysql://182.160.17.58:3306/opay_card_read";
        //查找的表名
        String table = "opay_apply";
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","Admin172.16.22.231");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");

        //SparkJdbc读取Postgresql的products表内容
        System.out.println("读取opay_card_read数据库中的opay_apply表内容");
        // 读取表中所有数据
        Dataset<Row> select = sqlContext.read().jdbc(url, table, connectionProperties).select("*");
        //显示数据
        select.show();
    }
}
