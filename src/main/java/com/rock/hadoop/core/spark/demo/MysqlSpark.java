package com.rock.hadoop.core.spark.demo;

import com.alibaba.fastjson.JSONObject;
import com.rock.hadoop.core.spark.vo.CardConfigDict;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.reflect.ClassTag;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 问题
 * 1、sql表达式如何来写？？--OK
 * 5、spark的各种使用场景及实践？？--正常用法对于spark
 */
public class MysqlSpark {
    public static void main(String[] args) {
        /**
         * master=local：表示单机本地运行
         * master=local[4]：表示单机本地4核运行
         * master=spark://master:7077：表示在一个spark standalone cluster 上运行
         */
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkMysql").master("local")
                .getOrCreate();
        SQLContext sqlContext = new SQLContext(spark);
        //读取mysql数据
//        readMethod1SQL(sqlContext);
        //
        readMethod2SQL(sqlContext);
        //读db数据
//        readSql(sqlContext);
        //写数据
//        writeDb(sqlContext,sparkContext);

//        readBySqlMethod(spark);

        //停止SparkContext
        spark.stop();
    }

    /**
     * todo 通过spark sql的方式来读取文件中的信息（sql的方式操作文件内容，不止db可以用sql）
     * @param sparkSession
     */
    private static void readBySqlMethod(SparkSession sparkSession){
        /**
         * 通过SparkSQL可以执行和SQL十分相似的查询操作--对文件或纯数据内容
         */

        //todo 1 、json自己带有schema结构，因此不需要手动去增加
        Dataset<Row> json = sparkSession.read().json("/Users/opayc/products/hadoop/src/main/java/com/rock/hadoop/core/spark/data/1.txt");
        json.select("age","name").where("age > 26").show();

        //todo 2、如果是一个txt文件，就需要在创建Dataset时手动塞入schema。先手动处理一下，之后在通过sql的方式进行操作
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext sc = new JavaSparkContext(sparkContext);
        JavaRDD<String> lines = sc.textFile("/Users/opayc/products/hadoop/src/main/java/com/rock/hadoop/core/spark/data/2.txt");
        //将String类型转化为Row类型-转换
        JavaRDD<Row> rowJavaRDD = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                String[] split = v1.split(" ");
                return RowFactory.create(
                        split[0],
                        Integer.valueOf(split[1])
                );
            }
        });
        //定义schema
        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
        );
        StructType structType = DataTypes.createStructType(structFields);
        //生成dataFrame--可以从rdd+HIVE表、或者spark数据源中创建一个DataFrame
        Dataset<Row> dataFrame = sparkSession.createDataFrame(rowJavaRDD, structType);
        dataFrame.select("name","age").where("age>24").orderBy(dataFrame.col("age").desc()).show();

    }

    /**
     * 自己写sql语句来从DB中获取数据--直接写sql语句
     * @param sqlContext
     */
    private static List<CardConfigDict> readSql(SQLContext sqlContext){
        //查找的表名
        String table = "card_config_dict";
        DataFrameReader reader = buildDataFrameReader(sqlContext, table);
        Dataset<Row> projectDataSourceDFFromMySQL = reader.load();
        projectDataSourceDFFromMySQL.createOrReplaceTempView(table);//注册临时表

        String sql = "select * from card_config_dict order by id desc limit 10"; //sql随便写，如果需要联查继续写就ok
        Dataset<Row> ss = projectDataSourceDFFromMySQL.sqlContext().sql(sql);

        //???
        Dataset<CardConfigDict> map = ss.map(new MapFunction<Row, CardConfigDict>() {
            @Override
            public CardConfigDict call(Row row) throws Exception {
                row.getLong(0);
                return null;
            }
        }, new Encoder<CardConfigDict>() {
            @Override
            public StructType schema() {
                return null;
            }

            @Override
            public ClassTag<CardConfigDict> clsTag() {
                return null;
            }
        });

        List<String> list =ss.toJSON().collectAsList();
        List<CardConfigDict> dataList = list.stream()
                .map(jsonString -> JSONObject.parseObject(jsonString, CardConfigDict.class))
                .collect(Collectors.toList());
        for(CardConfigDict cardConfigDict:dataList){
            System.out.println(cardConfigDict);
        }

        return dataList;
    }

    /**
     * 从DB中获取数据--这种方式需要熟悉spark sql的sql写法
     * @param sqlContext
     */
    private static void readMethod1SQL(SQLContext sqlContext){
        //查找的表名
        String table = "card_config_dict";
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = buildDbProperties();

        //SparkJdbc读取Postgresql的products表内容
        System.out.println("读取"+table+"表内容");
        // 读取表中所有数据
        Dataset<Row> dataset = sqlContext.read().jdbc(connectionProperties.getProperty("url"), table, connectionProperties);
        dataset.select("*").orderBy(dataset.col("id").desc());
        //显示数据--显示行数
        dataset.show(10);
    }

    /**
     * 方式2来从db中读取数据
     * @param sqlContext
     */
    private static void readMethod2SQL(SQLContext sqlContext){
        //查找的表名
        String table = "card_config_dict";
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties  properties= buildDbProperties();
        properties.put("dbtable",table);
        Map options=new HashMap<>(properties);

        Dataset<Row> jdbc = sqlContext.read().format("jdbc").options(options).load();
        jdbc.select("*").orderBy(jdbc.col("id").desc()).limit(10);//对结果进行sql处理，并不是在DB中进行sql处理
        jdbc.show();
    }

    /**
     * 往DB中写数据
     * @param sqlContext
     * @param sparkContext
     */
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

    private static DataFrameReader buildDataFrameReader(SQLContext sqlContext,String tableName){
        DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url", "jdbc:mysql://182.160.17.58:3306/opay_card_read");//数据库路径
        reader.option("driver", "com.mysql.jdbc.Driver");
        reader.option("user", "root");
        reader.option("password", "Admin172.16.22.231");

        reader.option("dbtable",tableName);
        return reader;
    }
}
