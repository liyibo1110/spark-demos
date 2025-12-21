package com.github.liyibo1110.spark.operator;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * filter算子使用示例
 * @author liyibo
 * @date 2025-12-19 15:37
 */
public class FilterDemo {
    private static final Logger logger = LoggerFactory.getLogger(FilterDemo.class);

    public void run(SparkSession spark) {
        List<Integer> list = List.of(1, 2, 3, 7, 4, 5, 8);
        Dataset<Integer> ds = spark.createDataset(list, Encoders.INT());
        Dataset<Integer> filteredDs = ds.filter((FilterFunction<Integer>) v -> v >= 4);
        logger.info("===== filter result =====");
        filteredDs.collectAsList().forEach(v -> logger.info("{}", v));
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("FilterDemo");
        try {
            new FilterDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
