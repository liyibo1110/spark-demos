package com.github.liyibo1110.spark.config;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liyibo
 * @date 2025-12-19 10:12
 */
public class SparkSessionFactory {
    private static final Logger logger = LoggerFactory.getLogger(SparkSessionFactory.class);

    /**
     * 构建SparkSession实例
     * @param appName application的名称
     * @return SparkSession实例
     */
    public static SparkSession create(String appName) {
        return SparkSession.builder()
                .appName(appName)
                .getOrCreate();
    }

    public static void main(String[] args) {
        logger.info("Factory已启动");
    }
}
