package com.pipeline.flink.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记实体类为业务事件实体。
 * <p>
 * 被标注的类对应的 Kafka topic 数据是纯业务 JSON（无信封层），
 * 直接用 EventDeserializationSchema 解析为目标类型。
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface EventEntity {
}
