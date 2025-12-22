package com.github.liyibo1110.spark.operator;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import com.github.liyibo1110.spark.utils.Constants;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

/**
 * reduceByKey算子使用示例（对具有相同key的value调性function来进行reduce操作）
 * 注意只有旧版的RDD才有reduceByKey这个方法，新版的Dataset应该组合使用groupByKey和reduceGroups来实现相同的功能
 * @author liyibo
 * @date 2025-12-22 09:41
 */
public class ReduceByKeyDemo {
    private static final Logger logger = LoggerFactory.getLogger(ReduceByKeyDemo.class);

    public void run(SparkSession spark) {
        Dataset<String> lines = spark.read().textFile(Constants.HDFS_DATA_PREFIX + "wordCount.txt");
        // 按空格先拆词
        Dataset<String> words = lines.flatMap(
                (FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator(),
                Encoders.STRING()
        );
        // 传统的mapper过程
        Dataset<Tuple2<String, Integer>> wordWithOne = words.map(
                (MapFunction<String, Tuple2<String, Integer>>) word -> new Tuple2<>(word, 1),
                Encoders.tuple(Encoders.STRING(), Encoders.INT())
        );
        // 新版对应的reduceByKey
        Dataset<Tuple2<String, Integer>> resultDs = wordWithOne.groupByKey(
                        (MapFunction<Tuple2<String, Integer>, String>) Tuple2::_1,
                        Encoders.STRING())
                .reduceGroups(
                        (ReduceFunction<Tuple2<String, Integer>>) (v1, v2) -> new Tuple2<>(v1._1, v1._2 + v2._2)
                )
                .map(
                        (MapFunction<Tuple2<String, Tuple2<String, Integer>>, Tuple2<String, Integer>>) Tuple2::_2,
                        Encoders.tuple(Encoders.STRING(), Encoders.INT())
                );
        logger.info("===== reduceByKey result =====");
        resultDs.collectAsList().forEach(t -> logger.info("{} -> {}", t._1, t._2));
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("ReduceByKeyDemo");
        try {
            new ReduceByKeyDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
