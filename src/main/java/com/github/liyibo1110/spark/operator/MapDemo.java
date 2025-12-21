package com.github.liyibo1110.spark.operator;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import com.github.liyibo1110.spark.operator.logic.StringSplitLogic;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * map算子使用示例
 * @author liyibo
 * @date 2025-12-19 11:40
 */
public class MapDemo {
    private static final Logger logger = LoggerFactory.getLogger(MapDemo.class);

    public void run(SparkSession spark) {
        List<String> list = List.of(
                "hello,bjsxt",
                "hello,xuruyun"
        );
        Dataset<String> ds = spark.createDataset(list, Encoders.STRING());
        Dataset<String[]> mappedDs = ds.map(
                (MapFunction<String, String[]>)StringSplitLogic::splitByComma,
                Encoders.javaSerialization(String[].class)
        );
        // 也包含了flatMap的使用，所以不需要再写一个FlatMapDemo了
        Dataset<String> flatMapDs = ds.flatMap(
                (FlatMapFunction<String, String>)StringSplitLogic::splitIteratorByComma,
                Encoders.STRING()
        );

        logger.info("===== map result =====");
        mappedDs.collectAsList()
                .forEach(arr -> {
                    for(String v : arr)
                        logger.info(v);
                });
        logger.info("===== flatMap result =====");
        flatMapDs.collectAsList().forEach(logger::info);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("MapDemo");
        try {
            new MapDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
