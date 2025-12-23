package com.github.liyibo1110.spark.operator;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * 笛卡尔积算子使用示例
 * @author liyibo
 * @date 2025-12-22 15:41
 */
public class CartesianDemo {
    private static final Logger logger = LoggerFactory.getLogger(CartesianDemo.class);

    public void run(SparkSession spark) {
        List<String> names = Arrays.asList("张三", "李四", "王五");
        List<Integer> scores = Arrays.asList(60, 70, 80);
        Dataset<String> nameDs = spark.createDataset(names, Encoders.STRING());
        Dataset<Integer> scoreDs = spark.createDataset(scores, Encoders.INT());

        Dataset<Row> cartesianDs = nameDs.toDF("name").crossJoin(scoreDs.toDF("score"));
        logger.info("===== cartesian result =====");
        cartesianDs.collectAsList().forEach(row -> {
            String name = row.getAs("name");
            Integer score = row.getAs("score");
            logger.info("{} -> {}", name, score);
        });
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("CartesianDemo");
        try {
            new CartesianDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
