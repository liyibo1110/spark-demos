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
 * 从parquet文件中读取数据
 * @author liyibo
 * @date 2025-12-25 12:18
 */
public class ParquetDataSourceDemo {
    private static final Logger logger = LoggerFactory.getLogger(ParquetDataSourceDemo.class);

    public void run(SparkSession spark) {
        /** 1、读取文件 */
        Dataset<Row> df = spark.read().parquet(Constants.HDFS_DATA_PREFIX + "users.parquet");
        logger.info("===== users schema =====");
        df.printSchema();
        logger.info("===== users show =====");
        df.show(false);
        /** 2、选择部分列 */
        Dataset<Row> nameAndColorDf = df.select("name", "favorite_color");
        /** 3、回写HDFS*/
        nameAndColorDf.write().mode(SaveMode.Overwrite).parquet(Constants.HDFS_DATA_PREFIX + "namesAndColors.parquet");
        /** 4、再次读取验证*/
        Dataset<Row> loaded = spark.read().parquet(Constants.HDFS_DATA_PREFIX + "namesAndColors.parquet");
        logger.info("===== loaded show =====");
        loaded.show(false);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("ParquetDataSourceDemo");
        try {
            new ParquetDataSourceDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
