package com.github.liyibo1110.spark.operator;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;

import static org.apache.spark.sql.functions.sum;

/**
 * aggregateByKey算子使用示例，新版使用groupBy和agg算子来代替
 * @author liyibo
 * @date 2025-12-22 15:25
 */
public class AggregateByKeyDemo {
    private static final Logger logger = LoggerFactory.getLogger(AggregateByKeyDemo.class);

    public void run(SparkSession spark) {
        List<Tuple2<Integer, Integer>> list = List.of(
                new Tuple2<>(1, 3),
                new Tuple2<>(1, 2),
                new Tuple2<>(1, 4),
                new Tuple2<>(2, 3));

        Dataset<Tuple2<Integer, Integer>> ds = spark.createDataset(list, Encoders.tuple(Encoders.INT(), Encoders.INT()));
        Dataset<Row> df = ds.toDF("key", "value");
        Dataset<Row> resultDf = df.groupBy("key").agg(sum("value").as("sum"));

        logger.info("===== aggregateByKey result =====");
        resultDf.collectAsList().forEach(r -> logger.info("{} -> {}", r.getInt(0), r.getLong(1)));
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("AggregateByKeyDemo");
        try {
            new AggregateByKeyDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
