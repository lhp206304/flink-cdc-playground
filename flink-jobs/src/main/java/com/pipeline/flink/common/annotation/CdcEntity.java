package com.pipeline.flink.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记实体类为 CDC 实体。
 * <p>
 * 被标注的类对应的 Kafka topic 数据是 Debezium CDC 信封格式
 * （包含 before/after/op/source 字段），需要用 CdcDeserializationSchema 解析。
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface CdcEntity {
}
