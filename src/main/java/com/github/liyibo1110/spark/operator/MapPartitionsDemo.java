package com.github.liyibo1110.spark.operator;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * mapPartitions算子使用示例，和普通map的区别是，直接对一个分区后的集合进行操作，而不是单个元素，因此输入输出都是Iterator
 * @author liyibo
 * @date 2025-12-22 13:19
 */
public class MapPartitionsDemo {
    private static final Logger logger = LoggerFactory.getLogger(MapPartitionsDemo.class);

    public void run(SparkSession spark) {
        List<String> names = Arrays.asList("张三1", "李四1", "王五1",
                                           "张三2", "李四2", "王五2",
                                           "张三3", "李四3", "王五3",
                                           "张三4");

        Dataset<String> ds = spark.createDataset(names, Encoders.STRING()).repartition(3);
        Dataset<String> resultDs = ds.mapPartitions(
                (MapPartitionsFunction<String, String>)iter -> {
                    List<String> list = new ArrayList<>();
                    int count = 0;  // 每个partition都是独立的变量
                    while(iter.hasNext())
                        list.add("count:" + count++ + "\t" + iter.next());
                    return list.iterator();
                },
                Encoders.STRING());

        logger.info("===== mapPartitions result =====");
        resultDs.collectAsList().forEach(logger::info);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("MapPartitionsDemo");
        try {
            new MapPartitionsDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
