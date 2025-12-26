package com.github.liyibo1110.spark.streaming;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import com.github.liyibo1110.spark.utils.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.split;

/**
 * 基于HDFS文件的流式WordCount
 * @author liyibo
 * @date 2025-12-25 14:39
 */
public class HDFSWordCountDemo {
    private static final Logger logger = LoggerFactory.getLogger(HDFSWordCountDemo.class);

    public void run(SparkSession spark) throws Exception {
        /** 1、启动读取指定目录 */
        Dataset<Row> lines = spark.readStream().format("text").load(Constants.HDFS_DATA_PREFIX + "wordCount_dir");
        /** 2、拆词 */
        Dataset<Row> words = lines.select(explode(split(col("value"), " ")).as("word"));
        /** 3、统计词频 */
        Dataset<Row> wordCounts = words.groupBy(col("word")).count();
        /** 4、输出到控制台*/
        wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", false)
                .start().awaitTermination();
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("HDFSWordCountDemo");
        try {
            new HDFSWordCountDemo().run(spark);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}
