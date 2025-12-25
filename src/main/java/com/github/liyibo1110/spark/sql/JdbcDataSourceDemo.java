package com.github.liyibo1110.spark.sql;

import com.github.liyibo1110.spark.config.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.round;

/**
 * 从mysql中读取数据并统计聚合，最后回写
 * @author liyibo
 * @date 2025-12-23 14:24
 */
public class JdbcDataSourceDemo {
    private static final Logger logger = LoggerFactory.getLogger(JdbcDataSourceDemo.class);

    public void run(SparkSession spark) {
        /** 1、JDBC基本配置 */
        String url = "jdbc:mysql://192.168.1.130:3307/spark_demo?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
        Properties props = this.initJdbcProperties();
        /** 2、读取原始数据 */
        Dataset<Row> usersDf = spark.read().jdbc(url, "users", props);
        logger.info("===== users schema =====");
        usersDf.printSchema();
        logger.info("===== users sample data =====");
        usersDf.show(5, false);
        /** 3、聚合统计计算 */
        Dataset<Row> resultDf = usersDf.filter(col("status").equalTo(1))    // status=1
                                       .groupBy(col("city"))                     // group by city
                                       .agg(                                              // 聚合相关列
                                               count(lit(1)).alias("user_count"),
                                               round(avg(col("age")), 2).alias("avg_age")
                                       )
                                       .withColumn("stat_time", current_timestamp());   // 新增stat_time列
        logger.info("===== aggregation result =====");
        resultDf.show(false);
        /** 4、写回stat表 */
        resultDf.write().mode("append").jdbc(url, "users_stat", props);
        logger.info("===== write to MySQL finished =====");
    }

    private Properties initJdbcProperties() {
        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "123456");
        props.put("driver", "com.mysql.cj.jdbc.Driver");
        return props;
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("JdbcUserStatDemo");
        try {
            new JdbcDataSourceDemo().run(spark);
        } finally {
            spark.stop();
        }
    }
}
