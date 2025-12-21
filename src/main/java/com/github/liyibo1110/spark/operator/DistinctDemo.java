package com.github.liyibo1110.spark.operator;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * distinct算子使用示例
 * @author liyibo
 * @date 2025-12-19 17:25
 */
public class DistinctDemo {
    private static final Logger logger = LoggerFactory.getLogger(DistinctDemo.class);

    public void run(SparkSession spark) {
        List<String> list = List.of("张三", "李四", "tom", "张三");
        Dataset<String> ds = spark.createDataset(list, Encoders.STRING());
        Dataset<String> uniqueDs  = ds.distinct();
        logger.info("===== distinct result =====");
        uniqueDs.collectAsList().forEach(logger::info);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("DistinctDemo");
        try {
            new DistinctDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
