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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;

/**
 * join算子使用示例
 * @author liyibo
 * @date 2025-12-22 12:15
 */
public class JoinDemo {
    private static final Logger logger = LoggerFactory.getLogger(JoinDemo.class);

    public void run(SparkSession spark) {
        List<Tuple2<Integer, String>> productList = List.of(
                new Tuple2<>(1, "苹果"),
                new Tuple2<>(2, "梨"),
                new Tuple2<>(3, "香蕉"),
                new Tuple2<>(4, "石榴"));


        List<Tuple2<Integer, Integer>> countList = List.of(
                new Tuple2<>(1, 7),
                new Tuple2<>(2, 3),
                new Tuple2<>(3, 8),
                new Tuple2<>(4, 3),
                new Tuple2<>(5, 9));

        Dataset<Tuple2<Integer, String>> productDs = spark.createDataset(productList, Encoders.tuple(Encoders.INT(), Encoders.STRING()));
        Dataset<Tuple2<Integer, Integer>> countDs = spark.createDataset(countList, Encoders.tuple(Encoders.INT(), Encoders.INT()));

        Dataset<Row> df1 = productDs.toDF("key", "name");
        Dataset<Row> df2 = countDs.toDF("key", "count");

        Dataset<Row> joinedDs = df1.join(df2, "key");
        // 按key聚合成集合
        Dataset<Row> aggDs = joinedDs.groupBy(col("key"))
                .agg(collect_list(col("name")).as("names"),
                     collect_list(col("count")).as("counts"));

        logger.info("===== join result =====");
        aggDs.collectAsList().forEach(row -> {
            Integer key = row.getInt(0);
            List<String> names = row.getList(1);
            List<Integer> counts = row.getList(2);
            logger.info("{} -> {} | {}", key, names, counts);
        });
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("JoinDemo");
        try {
            new JoinDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
