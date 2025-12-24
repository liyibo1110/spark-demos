package com.github.liyibo1110.spark.sharedvariables;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * 全局累加器
 * @author liyibo
 * @date 2025-12-22 16:14
 */
public class AccumulatorValueDemo {
    private static final Logger logger = LoggerFactory.getLogger(AccumulatorValueDemo.class);
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("AccumulatorDemo");
        // 创建累加器（运行在Driver端）
        LongAccumulator accumulator = spark.sparkContext().longAccumulator("MySumAccumulator");
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        Dataset<Integer> ds = spark.createDataset(list, Encoders.INT());
        // 累加器增加值（在Executor端里面加）
        ds.foreach((ForeachFunction<Integer>)n -> accumulator.add(n));
        // 读取累加器的值（只能在Driver端读）
        logger.info("Accumulator result: {}", accumulator.value());
        spark.stop();
    }
}
