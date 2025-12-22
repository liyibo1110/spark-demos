package com.github.liyibo1110.spark.operator;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 排序算子使用示例
 * @author liyibo
 * @date 2025-12-22 10:47
 */
public class SortByKeyDemo {
    private static final Logger logger = LoggerFactory.getLogger(SortByKeyDemo.class);

    public void run(SparkSession spark) {
        List<Integer> list = List.of(60, 70, 80, 55, 45, 75);
        Dataset<Integer> ds = spark.createDataset(list, Encoders.INT());
        // 正序
        Dataset<Integer> sortedAscDs = ds.orderBy("value");
        logger.info("===== sortAsc result =====");
        sortedAscDs.collectAsList().forEach(v -> logger.info("{}", v));
        // 降序
        Dataset<Integer> sortedDescDs = ds.orderBy(ds.col("value").desc());
        logger.info("===== sortDesc result =====");
        sortedDescDs.collectAsList().forEach(v -> logger.info("{}", v));

        // 以下是按特定key排序
        List<Tuple2<Integer, Integer>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>(3, 3));
        list2.add(new Tuple2<>(2, 2));
        list2.add(new Tuple2<>(1, 4));
        list2.add(new Tuple2<>(2, 3));
        Dataset<Tuple2<Integer, Integer>> ds2 = spark.createDataset(list2, Encoders.tuple(Encoders.INT(), Encoders.INT()));
        // 按照key升序
        Dataset<Tuple2<Integer, Integer>> ascDs = ds2.orderBy(ds2.col("_1"));
        logger.info("===== sortByKeyAsc result =====");
        ascDs.collectAsList().forEach(t -> logger.info("{} -> {}", t._1, t._2 ));
        // 按照key降序
        Dataset<Tuple2<Integer, Integer>> descDs = ds2.orderBy(ds2.col("_1").desc());
        logger.info("===== sortByKeyDesc result =====");
        descDs.collectAsList().forEach(t -> logger.info("{} -> {}", t._1, t._2 ));
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("SortDemo");
        try {
            new SortByKeyDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
