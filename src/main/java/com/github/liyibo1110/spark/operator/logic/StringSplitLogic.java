package com.github.liyibo1110.spark.operator.logic;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 字符串分隔基础算子
 * @author liyibo
 * @date 2025-12-19 13:51
 */
public final class StringSplitLogic {
    private StringSplitLogic() {

    }

    /**
     * 将给定字符串按照逗号分隔
     * @param line 一行文本
     * @return 分隔后的数组
     */
    public static String[] splitByComma(String line) {
        return line.split(",");
    }

    /**
     * 将给定字符串按照逗号分隔，返回迭代器实例
     * @param line 一行文本
     * @return 分隔后的集合的迭代器
     */
    public static Iterator<String> splitIteratorByComma(String line) {
        return Arrays.asList(splitByComma(line)).iterator();
    }
}
