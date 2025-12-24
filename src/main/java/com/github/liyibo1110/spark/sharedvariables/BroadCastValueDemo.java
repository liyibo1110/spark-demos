package com.github.liyibo1110.spark.sharedvariables;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * 广播变量
 * @author liyibo
 * @date 2025-12-23 11:04
 */
public class BroadCastValueDemo {
    private static final Logger logger = LoggerFactory.getLogger(BroadCastValueDemo.class);
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("BroadCastDemo");
        // 创建广播变量（运行在Driver端）
        int factor = 3;
        JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
        Broadcast<Integer> broadcastFactor = context.broadcast(factor);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        Dataset<Integer> ds = spark.createDataset(list, Encoders.INT());

        // 在Executor端使用广播变量
        Dataset<Integer> resultDs = ds.map(
                (MapFunction<Integer, Integer>) n -> n * broadcastFactor.value(),
                Encoders.INT()
        );

        // 在Driver端输出
        logger.info("===== broadcast result =====");
        resultDs.collectAsList().forEach(v -> logger.info("{}", v));
        spark.stop();
    }
}
