package com.github.liyibo1110.spark.operator;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * reduce算子使用示例
 * @author liyibo
 * @date 2025-12-19 17:07
 */
public class ReduceDemo {
    private static final Logger logger = LoggerFactory.getLogger(ReduceDemo.class);

    public void run(SparkSession spark) {
        List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        Dataset<Integer> ds = spark.createDataset(list, Encoders.INT());
        Integer sum = ds.reduce((ReduceFunction<Integer>) (num1, num2) -> num1 + num2);
        logger.info("===== reduce result =====");
        logger.info("{}", sum);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("ReduceDemo");
        try {
            new ReduceDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
