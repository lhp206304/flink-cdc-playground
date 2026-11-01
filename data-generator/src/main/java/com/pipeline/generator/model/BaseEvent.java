package com.pipeline.generator.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import lombok.Getter;

import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

/**
 * 事件基类。
 *
 * <p>所有业务事件（订单、点击、价格变更）都继承此类，统一维护公共字段。
 * 这些公共字段是 Iceberg 表的分区键和主键，必须在生成时就确定。</p>
 */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class BaseEvent {

    /** 事件唯一标识。主键字段，可能被复用（模拟 upsert 场景）。 */
    private final String eventId;

    /** 事件发生时间。用于 Watermark 和迟到事件检测。 */
    @JsonSerialize(using = InstantSerializer.class)
    private final Instant eventTime;

    /** 事件所属区域。用于分区。取值如 us-east-1。 */
    private final String region;

    /** 事件发生日期（UTC）。Iceberg 按天分区的分区键。 */
    @JsonSerialize(using = LocalDateSerializer.class)
    private final LocalDate eventDate;

    /** 事件类型标识。用于 Kafka 消费端区分不同事件。 */
    private final String eventType;

    /** 元数据扩展字段。存放 source、schema_version 等附加信息。 */
    private final Map<String, String> metadata = new HashMap<>();

    protected BaseEvent(String eventId, Instant eventTime, String region,
                        LocalDate eventDate, String eventType) {
        this.eventId = eventId;
        this.eventTime = eventTime;
        this.region = region;
        this.eventDate = eventDate;
        this.eventType = eventType;
    }

    /** 向元数据中追加一个键值对。 */
    public void addMetadata(String key, String value) {
        metadata.put(key, value);
    }
}
