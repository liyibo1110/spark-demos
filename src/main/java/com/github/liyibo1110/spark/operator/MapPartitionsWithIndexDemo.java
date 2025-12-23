package com.github.liyibo1110.spark.operator;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.spark_partition_id;

/**
 * mapPartitionsWithIndex算子使用示例，和mapPartitions的区别是，输入时会多提供一个整数表示分区的编号
 * 但是在新版的Dataset中，并没有mapPartitionsWithIndex方法，所以只能使用spark_partition_id方法来代替
 * @author liyibo
 * @date 2025-12-22 13:19
 */
public class MapPartitionsWithIndexDemo {
    private static final Logger logger = LoggerFactory.getLogger(MapPartitionsWithIndexDemo.class);

    public void run(SparkSession spark) {
        List<String> nameList = Arrays.asList("张三1", "李四1", "王五1",
                                           "张三2", "李四2", "王五2",
                                           "张三3", "李四3", "王五3",
                                           "张三4");

        Dataset<String> ds = spark.createDataset(nameList, Encoders.STRING()).repartition(3);
        Dataset<Row> pids = ds.toDF("name")
                              .withColumn("pid", spark_partition_id())
                              .orderBy("pid", "name")
                              .groupBy("pid")
                              .agg(collect_list("name").as("names"));

        logger.info("===== mapPartitionsWithIndex result =====");
        pids.collectAsList().forEach(row -> {
            Integer pid = row.getAs("pid");
            Seq<String> scalaSeq = row.getAs("names");
            List<String> names = JavaConverters.seqAsJavaList(scalaSeq);
            logger.info("分区索引：{} -> {}", pid, names);
        });
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("MapPartitionsWithIndexDemo");
        try {
            new MapPartitionsWithIndexDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
