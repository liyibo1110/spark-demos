package com.github.liyibo1110.spark.operator;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * sample算子使用示例
 * @author liyibo
 * @date 2025-12-22 16:02
 */
public class SampleAndTakeDemo {
    private static final Logger logger = LoggerFactory.getLogger(SampleAndTakeDemo.class);

    public void run(SparkSession spark) {
        List<Integer> list = List.of(1, 2, 3, 7, 4, 5, 8);
        Dataset<Integer> ds = spark.createDataset(list, Encoders.INT());
        Dataset<Integer> sampleDs = ds.sample(false, 0.5, System.currentTimeMillis());
        logger.info("===== sample result =====");
        sampleDs.collectAsList().forEach(v -> logger.info("{}", v));
        // 取前N条（不随机）
        logger.info("===== take(3) result =====");
        ds.takeAsList(3).forEach(v -> logger.info("{}", v));
        // 随机取3条
        logger.info("===== random take(3) result =====");
        Dataset<Integer> limitSampleDs = ds.sample(false, 1.0, System.currentTimeMillis())
                .limit(3);
        limitSampleDs.collectAsList().forEach(v -> logger.info("{}", v));
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("SampleDemo");
        try {
            new SampleAndTakeDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
