package com.github.liyibo1110.spark.operator;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * intersection算子使用示例
 * @author liyibo
 * @date 2025-12-22 09:07
 */
public class IntersectionDemo {
    private static final Logger logger = LoggerFactory.getLogger(IntersectionDemo.class);

    public void run(SparkSession spark) {
        List<String> list1 = List.of("张三", "李四", "tom");
        List<String> list2 = List.of("tom", "张三");
        Dataset<String> ds1 = spark.createDataset(list1, Encoders.STRING());
        Dataset<String> ds2 = spark.createDataset(list2, Encoders.STRING());
        // 取交集
        Dataset<String> intersectedDs = ds1.intersect(ds2);
        logger.info("===== intersected result =====");
        intersectedDs.collectAsList().forEach(logger::info);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("IntersectionDemo");
        try {
            new IntersectionDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
