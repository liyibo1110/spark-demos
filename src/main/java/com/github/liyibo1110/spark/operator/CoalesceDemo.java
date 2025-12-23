package com.github.liyibo1110.spark.operator;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * coalesce算子使用示例，
 * @author liyibo
 * @date 2025-12-22 14:57
 */
public class CoalesceDemo {
    private static final Logger logger = LoggerFactory.getLogger(CoalesceDemo.class);

    public void run(SparkSession spark) {
        List<String> words = Arrays.asList("hi", "hello", "how", "are", "you");

        Dataset<String> ds = spark.createDataset(words, Encoders.STRING()).repartition(4);
        logger.info("原始分区数：{}", ds.rdd().getNumPartitions());
        // 使用coalesce来减少分区数（不进行shuffle）
        Dataset<String> ds2 = ds.coalesce(2);   // shuffle参数默认就是false
        logger.info("coalesce后的分区数：{}", ds2.rdd().getNumPartitions());
        logger.info("===== coalesce result =====");
        ds2.collectAsList().forEach(logger::info);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("CoalesceDemo");
        try {
            new CoalesceDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
