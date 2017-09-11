package com.date;

import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.zone.ZoneRules;

/**
 * Created by zhoujun on 2017/9/11.
 */
public class DateTest {
    // 默认时间格式
    private static final DateTimeFormatter DEFAULT_DATETIME_FORMATTER = TimeFormat.SHORT_DATE_PATTERN_LINE.formatter;

    // 无参数的构造函数,防止被实例化
    private DateTest(){}

    /**
     * String 转化为 LocalDateTime
     * @param timeStr  被转化的字符串
     * @return LocalDateTime
     */
    public static LocalDateTime parseTime(String timeStr) {
        return LocalDateTime.parse(timeStr, DEFAULT_DATETIME_FORMATTER);

    }

    /**
     * String 转化为 LocalDateTime
     * @param timeStr 被转化的字符串
     * @param timeFormat  转化的时间格式
     * @return LocalDateTime
     */
    public static LocalDateTime parseTime(String timeStr, TimeFormat timeFormat) {
        return LocalDateTime.parse(timeStr, timeFormat.formatter);

    }

    /**
     * LocalDateTime 转化为String
     * @param time   LocalDateTime
     * @return String
     */
    public static String parseTime(LocalDateTime time) {
        return DEFAULT_DATETIME_FORMATTER.format(time);

    }

    /**
     * LocalDateTime 时间转 String
     * @param time LocalDateTime
     * @param format  时间格式
     * @return String
     */
    public static String parseTime(LocalDateTime time, TimeFormat format) {
        return format.formatter.format(time);
    }

    /**
     * 获取当前时间
     * @return
     */
    public static String getCurrentDateTime() {
        return DEFAULT_DATETIME_FORMATTER.format(LocalDateTime.now());
    }

    /**
     * 获取当前时间
     * @param timeFormat 时间格式
     * @return
     */
    public static String getCurrentDateTime(TimeFormat timeFormat) {
        return timeFormat.formatter.format(LocalDateTime.now());
    }

    /**
     * 内部枚举类
     * @author xiaowen
     * @date 2016年11月1日 @ version 1.0
     */
    public enum TimeFormat {
        //短时间格式 年月日
        /**
         *时间格式：yyyy-MM-dd
         */
        SHORT_DATE_PATTERN_LINE("yyyy-MM-dd"),
        /**
         *时间格式：yyyy/MM/dd
         */
        SHORT_DATE_PATTERN_SLASH("yyyy/MM/dd"),
        /**
         *时间格式：yyyy\\MM\\dd
         */
        SHORT_DATE_PATTERN_DOUBLE_SLASH("yyyy\\MM\\dd"),
        /**
         *时间格式：yyyyMMdd
         */
        SHORT_DATE_PATTERN_NONE("yyyyMMdd"),

        // 长时间格式 年月日时分秒
        /**
         *时间格式：yyyy-MM-dd HH:mm:ss
         */
        LONG_DATE_PATTERN_LINE("yyyy-MM-dd HH:mm:ss"),

        /**
         *时间格式：yyyy/MM/dd HH:mm:ss
         */
        LONG_DATE_PATTERN_SLASH("yyyy/MM/dd HH:mm:ss"),
        /**
         *时间格式：yyyy\\MM\\dd HH:mm:ss
         */
        LONG_DATE_PATTERN_DOUBLE_SLASH("yyyy\\MM\\dd HH:mm:ss"),
        /**
         *时间格式：yyyyMMdd HH:mm:ss
         */
        LONG_DATE_PATTERN_NONE("yyyyMMdd HH:mm:ss"),

        // 长时间格式 年月日时分秒 带毫秒
        LONG_DATE_PATTERN_WITH_MILSEC_LINE("yyyy-MM-dd HH:mm:ss.SSS"),

        LONG_DATE_PATTERN_WITH_MILSEC_SLASH("yyyy/MM/dd HH:mm:ss.SSS"),

        LONG_DATE_PATTERN_WITH_MILSEC_DOUBLE_SLASH("yyyy\\MM\\dd HH:mm:ss.SSS"),

        LONG_DATE_PATTERN_WITH_MILSEC_NONE("yyyyMMdd HH:mm:ss.SSS");

        private transient DateTimeFormatter formatter;

        TimeFormat(String pattern) {
            formatter = DateTimeFormatter.ofPattern(pattern);

        }
    }
    /**
     * 测试
     * @param args
     */
    public static void main(String[] args) {
        //获取当前时间
        log(DateTest.getCurrentDateTime());

        log(DateTest.getCurrentDateTime(TimeFormat.LONG_DATE_PATTERN_LINE));
    }

    public static void log(Object msg){
        System.out.println(" 信息: "+ msg.getClass() + "  ::: " +  msg);
    }
}
