package com.github.liyibo1110.spark.operator;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * union算子使用示例
 * @author liyibo
 * @date 2025-12-19 17:25
 */
public class UnionDemo {
    private static final Logger logger = LoggerFactory.getLogger(UnionDemo.class);

    public void run(SparkSession spark) {
        List<String> list1 = List.of("张三", "李四");
        List<String> list2 = List.of("tom", "张三");
        Dataset<String> ds1 = spark.createDataset(list1, Encoders.STRING());
        Dataset<String> ds2 = spark.createDataset(list2, Encoders.STRING());
        // 不会自动去重
        Dataset<String> unionDs = ds1.union(ds2);
        logger.info("===== union result =====");
        unionDs.collectAsList().forEach(logger::info);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("UnionDemo");
        try {
            new UnionDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
