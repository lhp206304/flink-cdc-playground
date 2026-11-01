package com.pipeline.flink.common.serde;

import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.pipeline.flink.common.JsonUtils;

/**
 * 业务事件反序列化器。
 * <p>
 * 将 Kafka 的 events.* topic 中的纯业务 JSON 直接反序列化为目标类型 T（无信封层）。
 * 解析失败时返回 failure 结果，原始消息交由下游侧输出到 DLQ。
 *
 * @param <T> 业务事件类型（如 ClickEvent、ProductPriceEvent）
 */
public class EventDeserializationSchema<T> extends AbstractDeserializationSchema<DeserializationResult<T>> {

    private static final long serialVersionUID = 1L;

    /** 目标事件的 Class。 */
    private final Class<T> eventClass;

    public EventDeserializationSchema(Class<T> eventClass) {
        this.eventClass = eventClass;
    }

    @Override
    public DeserializationResult<T> deserialize(byte[] message) {
        if (message == null || message.length == 0) {
            return DeserializationResult.failure(null);
        }
        try {
            T event = JsonUtils.MAPPER.readValue(message, eventClass);
            return DeserializationResult.success(event);
        } catch (Exception e) {
            // 解析失败，保留原始 JSON 字符串，交由下游侧输出到 DLQ
            String raw = new String(message, StandardCharsets.UTF_8);
            return DeserializationResult.failure(raw);
        }
    }

    @Override
    public TypeInformation<DeserializationResult<T>> getProducedType() {
        super.getProducedType();

        return TypeInformation.of(new TypeHint<DeserializationResult<T>>() {});
    }


}
