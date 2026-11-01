package com.pipeline.flink.common;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import com.pipeline.flink.common.annotation.CdcEntity;
import com.pipeline.flink.common.annotation.EventEntity;
import com.pipeline.flink.common.serde.CdcDeserializationSchema;
import com.pipeline.flink.common.serde.DeserializationResult;
import com.pipeline.flink.common.serde.EventDeserializationSchema;
import com.pipeline.flink.kafka.model.CdcEnvelope;

/**
 * Kafka Source 工厂。
 * <p>
 * 根据实体类上的注解（{@link CdcEntity} / {@link EventEntity}）自动选择反序列化器：
 * <ul>
 *   <li>@CdcEntity → 使用 CdcDeserializationSchema，返回 CdcEnvelope&lt;T&gt;</li>
 *   <li>@EventEntity → 使用 EventDeserializationSchema，返回 T</li>
 * </ul>
 * 解析失败的数据通过 DeserializationResult 标记，由下游侧输出到 DLQ。
 */
public class KafkaSourceFactory {

    private KafkaSourceFactory() {
    }

    /**
     * 构建 String 类型的 Kafka Source（调试用，不做 JSON 解析）。
     *
     * @param topic   Kafka topic
     * @param offsets 起始消费位点
     * @return KafkaSource<String>
     */
    public static KafkaSource<String> buildStringSource(String topic, OffsetsInitializer offsets) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(EnvConfig.getRequired("KAFKA_BOOTSTRAP_SERVERS"))
                .setTopics(topic)
                .setGroupId(EnvConfig.getRequired("KAFKA_GROUP_ID"))
                .setStartingOffsets(offsets)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    /**
     * 构建 String 类型的 Kafka Source，默认从最新位点开始消费。
     */
    public static KafkaSource<String> buildStringSource(String topic) {
        return buildStringSource(topic, OffsetsInitializer.latest());
    }

    /**
     * 自动识别实体类型并构建 Kafka Source。
     * <p>
     * 根据 clazz 上的注解决定反序列化方式：
     * <ul>
     *   <li>@CdcEntity → 解析 Debezium CDC 信封，返回 CdcEnvelope&lt;T&gt;</li>
     *   <li>@EventEntity → 直接解析业务 JSON，返回 T</li>
     * </ul>
     *
     * @param topic Kafka topic
     * @param clazz 实体类（必须标注 @CdcEntity 或 @EventEntity）
     * @param <T>   实体类型
     * @return KafkaSource（CDC 为 CdcEnvelope<T>，事件为 T）
     */
    @SuppressWarnings("unchecked")
    public static <T> KafkaSource<?> buildSource(String topic, Class<T> clazz) {
        if (clazz.isAnnotationPresent(CdcEntity.class)) {
            return buildCdcSource(topic, clazz);
        }
        if (clazz.isAnnotationPresent(EventEntity.class)) {
            return buildEventSource(topic, clazz);
        }
        throw new IllegalArgumentException(
                "类 " + clazz.getName() + " 必须标注 @CdcEntity 或 @EventEntity");
    }

    /**
     * 构建 CDC 类型的 Kafka Source。
     * <p>
     * 消费 cdc.* topic，将 Debezium JSON 解析为 {@link CdcEnvelope}。
     *
     * @param topic Kafka topic（如 cdc.orders）
     * @param clazz 业务实体类（如 Order.class）
     * @param <T>   实体类型
     * @return KafkaSource<DeserializationResult<CdcEnvelope<T>>>
     */
    public static <T> KafkaSource<DeserializationResult<CdcEnvelope<T>>> buildCdcSource(
            String topic, Class<T> clazz) {
        CdcDeserializationSchema<T> schema = new CdcDeserializationSchema<>(clazz);
        return KafkaSource.<DeserializationResult<CdcEnvelope<T>>>builder()
                .setBootstrapServers(EnvConfig.getRequired("KAFKA_BOOTSTRAP_SERVERS"))
                .setTopics(topic)
                .setGroupId(EnvConfig.getRequired("KAFKA_GROUP_ID"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(schema)
                .build();
    }

    /**
     * 构建业务事件类型的 Kafka Source。
     * <p>
     * 消费 events.* topic，将纯业务 JSON 直接解析为目标类型。
     *
     * @param topic Kafka topic（如 events.clicks）
     * @param clazz 事件实体类（如 ClickEvent.class）
     * @param <T>   事件类型
     * @return KafkaSource<DeserializationResult<T>>
     */
    public static <T> KafkaSource<DeserializationResult<T>> buildEventSource(
            String topic, Class<T> clazz) {
        EventDeserializationSchema<T> schema = new EventDeserializationSchema<>(clazz);
        return KafkaSource.<DeserializationResult<T>>builder()
                .setBootstrapServers(EnvConfig.getRequired("KAFKA_BOOTSTRAP_SERVERS"))
                .setTopics(topic)
                .setGroupId(EnvConfig.getRequired("KAFKA_GROUP_ID"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(schema)
                .build();
    }

    /**
     * 构建 DLQ（死信队列）Kafka Sink。
     * <p>
     * 将反序列化失败的原始消息（String）写入 DLQ topic，便于后续排查和重放。
     * 投递语义：AT_LEAST_ONCE（至少一次，允许重复，不丢数据）。
     *
     * @return KafkaSink<String>
     */
    public static KafkaSink<String> buildDlqSink() {
        String topic = EnvConfig.getRequired("KAFKA_DLQ_TOPIC");
        return KafkaSink.<String>builder()
                .setBootstrapServers(EnvConfig.getRequired("KAFKA_BOOTSTRAP_SERVERS"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
    }
}
