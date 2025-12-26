package com.github.liyibo1110.spark.sql;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import com.github.liyibo1110.spark.utils.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * 从json文件中读取数据
 * @author liyibo
 * @date 2025-12-25 10:09
 */
public class JsonDataSourceDemo {
    private static final Logger logger = LoggerFactory.getLogger(JsonDataSourceDemo.class);

    public void run(SparkSession spark) {
        /** 1、读取文件 */
        Dataset<Row> ds = spark.read().format("json").load(Constants.HDFS_DATA_PREFIX + "people.json");
        logger.info("===== people schema =====");
        ds.printSchema();
        /** 2、创建临时视图 */
        ds.createOrReplaceTempView("people");
        /** 3、使用Spark SQL查询 */
        Dataset<Row> teenagers = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
        logger.info("===== teenagers show =====");
        teenagers.show(false);
        /** 4、再尝试直接在内存构造Json数据 */
        List<String> personJsonList = Arrays.asList(
                "{\"name\":\"ZhangFa\",\"age\":32}",
                "{\"name\":\"Faker\",\"age\":12}",
                "{\"name\":\"Moon\",\"age\":62}"
        );
        Dataset<String> jsonDs = spark.createDataset(personJsonList, Encoders.STRING());
        Dataset<Row> studentDf = spark.read().json(jsonDs);
        /** 5、创建新的临时视图 */
        studentDf.createOrReplaceTempView("student");
        Dataset<Row> students = spark.sql("SELECT * FROM student");
        logger.info("===== students show =====");
        students.show(false);
        /** 6、回写HDFS */
        students.write().mode(SaveMode.Overwrite).format("json").save(Constants.HDFS_DATA_PREFIX + "student");
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("JsonDataSourceDemo");
        try {
            new JsonDataSourceDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
