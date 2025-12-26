package com.github.liyibo1110.spark.streaming;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import com.github.liyibo1110.spark.utils.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.split;

/**
 * 基于Kafka文件的流式WordCount（其实只有Direct方式了，不再有Receiver方式）
 * @author liyibo
 * @date 2025-12-25 17:47
 */
public class KafkaWordCountDemo {
    private static final Logger logger = LoggerFactory.getLogger(KafkaWordCountDemo.class);

    public void run(SparkSession spark) throws Exception {
        /** 1、从Kafka读 */
        Dataset<Row> kafkaDf = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "master:9092,master:9093,master:9094")
                .option("subscribe", "wordcount_topic")
                .option("startingOffsets", "latest").load();
        /** 2、将收到的byte[]，转成String */
        Dataset<String> lines = kafkaDf.selectExpr("CAST(value AS STRING)").as(Encoders.STRING());
        /** 3、统计词频 */
        Dataset<Row> wordCounts = lines.select(explode(split(col("value"), " "))
                                  .as("word"))
                                  .groupBy("word")
                                  .count();
        /** 4、输出到控制台*/
        wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", false)
                .option("checkpointLocation", Constants.HDFS_DATA_PREFIX + "checkpoints/kafka-wordcount")
                .start().awaitTermination();
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("KafkaWordCountDemo");
        try {
            new KafkaWordCountDemo().run(spark);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}
