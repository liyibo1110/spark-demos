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

import static org.apache.spark.sql.functions.*;

/**
 * coGroup算子使用示例，新版API没有直接cogroup的API，需要利用join和group算子来实现相同的功能
 * @author liyibo
 * @date 2025-12-22 11:07
 */
public class CoGroupDemo {
    private static final Logger logger = LoggerFactory.getLogger(CoGroupDemo.class);

    public void run(SparkSession spark) {
        List<Tuple2<Integer, String>> list1 = List.of(
                new Tuple2<>(1, "苹果"),
                new Tuple2<>(2, "梨"),
                new Tuple2<>(3, "香蕉"),
                new Tuple2<>(4, "石榴"));


        List<Tuple2<Integer, Integer>> list2 = List.of(
                new Tuple2<>(1, 7),
                new Tuple2<>(2, 3),
                new Tuple2<>(3, 8),
                new Tuple2<>(4, 3));


        List<Tuple2<Integer, String>> list3 = List.of(
                new Tuple2<>(1, "7"),
                new Tuple2<>(2, "3"),
                new Tuple2<>(3, "8"),
                new Tuple2<>(4, "3"),
                new Tuple2<>(4, "4"),
                new Tuple2<>(4, "5"),
                new Tuple2<>(4, "6"));

        Dataset<Tuple2<Integer, String>> ds1 = spark.createDataset(list1, Encoders.tuple(Encoders.INT(), Encoders.STRING()));
        Dataset<Tuple2<Integer, Integer>> ds2 = spark.createDataset(list2, Encoders.tuple(Encoders.INT(), Encoders.INT()));
        Dataset<Tuple2<Integer, String>> ds3 = spark.createDataset(list3, Encoders.tuple(Encoders.INT(), Encoders.STRING()));

        Dataset<Row> df1 = ds1.toDF("key", "fruit");
        Dataset<Row> df2 = ds2.toDF("key", "num");
        Dataset<Row> df3 = ds3.toDF("key", "score");

        Dataset<Row> joinedDs = df1.join(df2, "key").join(df3, "key");
        // 按key聚合成集合
        Dataset<Row> aggDs = joinedDs.groupBy(col("key"))
                .agg(collect_list(col("fruit")).as("fruits"),
                     collect_list(col("num")).as("nums"),
                     collect_list(col("score")).as("scores"));

        logger.info("===== coGroup result =====");
        aggDs.collectAsList().forEach(row -> {
            Integer key = row.getInt(0);
            List<String> fruits = row.getList(1);
            List<Integer> nums = row.getList(2);
            List<String> scores = row.getList(3);
            logger.info("{} -> {} | {} | {}", key, fruits, nums, scores);
        });
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("CoGroupDemo");
        try {
            new CoGroupDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
