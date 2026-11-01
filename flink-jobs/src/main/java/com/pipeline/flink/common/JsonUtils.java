package com.pipeline.flink.common;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * JSON 工具类。
 * <p>
 * 提供全局共享的 {@link ObjectMapper} 单例，供所有需要 JSON 序列化/反序列化的类复用。
 * <p>
 * 为什么用单例：
 * <ul>
 *   <li>ObjectMapper 创建成本高（内部缓存序列化器/反序列化器）</li>
 *   <li>ObjectMapper 线程安全，可多线程共享</li>
 *   <li>全局统一配置，避免各处配置不一致</li>
 * </ul>
 */
public final class JsonUtils {

    /**
     * 兼容 PostgreSQL timestamp 文本格式的 DateTimeFormatter。
     * <p>
     * PostgreSQL CDC 输出的时间格式为 {@code yyyy-MM-dd HH:mm:ss[.SSSSSS]}
     * （空格分隔日期和时间，秒后可带 0~6 位小数）。
     * 同时兼容 ISO-8601 标准格式 {@code yyyy-MM-ddTHH:mm:ss[.SSSSSS]}。
     */
    private static final DateTimeFormatter FLEXIBLE_LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            // 日期与时间之间支持空格或 T 分隔
            .optionalStart().appendLiteral(' ').optionalEnd()
            .optionalStart().appendLiteral('T').optionalEnd()
            .appendPattern("HH:mm:ss")
            // 秒后可带 1~6 位小数（纳秒精度）
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
            .optionalEnd()
            .toFormatter();

    /** 全局共享的 ObjectMapper 单例。 */
    public static final ObjectMapper MAPPER = buildMapper();

    private static ObjectMapper buildMapper() {
        ObjectMapper mapper = new ObjectMapper()
                // 未知字段不报错，直接忽略（Debezium CDC 消息有多余字段）
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                // 注册 JavaTimeModule（提供 java.time 类型支持）
                .registerModule(new JavaTimeModule());

        // 注册自定义 LocalDateTime 反序列化器，兼容 PostgreSQL 时间格式
        SimpleModule customModule = new SimpleModule();
        customModule.addDeserializer(LocalDateTime.class, new FlexibleLocalDateTimeDeserializer());
        mapper.registerModule(customModule);

        return mapper;
    }

    private JsonUtils() {
        // 工具类禁止实例化
    }

    /**
     * 灵活的 LocalDateTime 反序列化器。
     * <p>
     * 支持以下格式：
     * <ul>
     *   <li>{@code 2026-10-28 22:13:39}（PostgreSQL 默认，无小数秒）</li>
     *   <li>{@code 2026-10-28 22:13:39.807295}（PostgreSQL microsecond 精度）</li>
     *   <li>{@code 2026-10-28T22:13:39.807295}（ISO-8601 标准格式）</li>
     * </ul>
     */
    private static class FlexibleLocalDateTimeDeserializer extends JsonDeserializer<LocalDateTime> {
        @Override
        public LocalDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            String text = p.getValueAsString();
            if (text == null || text.isEmpty()) {
                return null;
            }
            try {
                return LocalDateTime.parse(text, FLEXIBLE_LOCAL_DATE_TIME);
            } catch (Exception e) {
                throw ctxt.weirdStringException(text, LocalDateTime.class,
                        "无法解析为 LocalDateTime，支持格式: yyyy-MM-dd[ T]HH:mm:ss[.SSSSSS]");
            }
        }
    }
}
