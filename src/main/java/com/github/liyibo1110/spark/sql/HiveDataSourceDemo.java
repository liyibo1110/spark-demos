package com.github.liyibo1110.spark.sql;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 此demo未成功，hadoop + spark + hive + jdk之间的版本依赖关系比较复杂，高级的spark需要低级的hive才能用，而低级的hive则需要低级的jdk
 * @author liyibo
 * @date 2025-12-24 15:30
 */
public class HiveDataSourceDemo {
    private static final Logger logger = LoggerFactory.getLogger(HiveDataSourceDemo.class);

    public void run(SparkSession spark) {
        /** 1、直接执行sql */
        /*Dataset<Row> databases = spark.read().format("jdbc")
                .option("url", "jdbc:hive2://master:10000/default")
                .option("driver", "org.apache.hive.jdbc.HiveDriver")
                //.option("query", "show databases")
                .option("dbtable", "(SELECT name FROM sys.databases) t")
                .load();*/
        Dataset<Row> databases = spark.sql("SHOW DATABASES");
        /** 2、输出结果 */
        databases.show(false);
        /** 3、在Driver端获取结果 */
        List<Row> rows = databases.collectAsList();
        logger.info("=======================================");
        for(Row row : rows)
            logger.info("Database: {}", row.getString(0));
        logger.info("=======================================");
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createWithHive("HiveDataSourceDemo");
        try {
            new HiveDataSourceDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
