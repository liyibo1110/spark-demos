package com.github.liyibo1110.spark.sql;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import com.github.liyibo1110.spark.utils.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件格式转换
 * @author liyibo
 * @date 2025-12-25 13:58
 */
public class FormatChangeDemo {
    private static final Logger logger = LoggerFactory.getLogger(FormatChangeDemo.class);

    public void run(SparkSession spark) {
        /** 1、读取json文件 */
        Dataset<Row> df = spark.read().format("json").load(Constants.HDFS_DATA_PREFIX + "people.json");
        logger.info("===== people schema =====");
        df.printSchema();
        logger.info("===== people show =====");
        df.show(false);
        /** 2、选择部分列 */
        Dataset<Row> nameDf = df.select("name");
        nameDf.write().mode(SaveMode.Overwrite).parquet(Constants.HDFS_DATA_PREFIX + "people.parquet");
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("FormatChangeDemo");
        try {
            new FormatChangeDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
