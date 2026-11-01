package com.pipeline.flink.common.serde;

import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.databind.type.TypeFactory;
import com.pipeline.flink.common.JsonUtils;
import com.pipeline.flink.kafka.model.CdcEnvelope;

/**
 * CDC 数据反序列化器。
 * <p>
 * 将 Kafka 的 cdc.* topic 中的 Debezium JSON 反序列化为 {@link CdcEnvelope}。
 * 解析失败时返回 failure 结果，原始消息交由下游侧输出到 DLQ。
 *
 * @param <T> 业务实体类型（如 Order、Payment）
 */
public class CdcDeserializationSchema<T>
        extends AbstractDeserializationSchema<DeserializationResult<CdcEnvelope<T>>> {

    private static final long serialVersionUID = 1L;

    /** 业务实体的 Class，用于构建泛型反序列化类型。 */
    private final Class<T> entityClass;

    public CdcDeserializationSchema(Class<T> entityClass) {
        this.entityClass = entityClass;
    }

    @Override
    public DeserializationResult<CdcEnvelope<T>> deserialize(byte[] message) {
        if (message == null || message.length == 0) {
            return DeserializationResult.failure(null);
        }
        try {
            // 构建 CdcEnvelope<T> 的泛型类型
            TypeFactory typeFactory = JsonUtils.MAPPER.getTypeFactory();
            var envelopeType = typeFactory.constructParametricType(CdcEnvelope.class, entityClass);
            CdcEnvelope<T> envelope = JsonUtils.MAPPER.readValue(message, envelopeType);
            return DeserializationResult.success(envelope);
        } catch (Exception e) {
            // 解析失败，保留原始 JSON 字符串，交由下游侧输出到 DLQ
            String raw = new String(message, StandardCharsets.UTF_8);
            return DeserializationResult.failure(raw);
        }
    }

    @Override
    public TypeInformation<DeserializationResult<CdcEnvelope<T>>> getProducedType() {
        return TypeInformation.of(new TypeHint<DeserializationResult<CdcEnvelope<T>>>() {});
    }

}
