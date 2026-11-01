package com.pipeline.flink.kafka.model.event;

import java.time.Instant;
import java.time.LocalDate;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 业务事件基类。
 * <p>
 * 所有业务事件（点击、价格变更）都继承此类，统一维护公共字段。
 * 这些公共字段来自 Kafka 的 events.* topic，格式与 data-generator 输出一致。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class BaseEvent {

    /** 事件唯一标识。 */
    private String eventId;

    /** 事件发生时间（ISO-8601 格式）。用于 Watermark 和迟到事件检测。 */
    private Instant eventTime;

    /** 事件所属区域，用于分区。 */
    private String region;

    /** 事件发生日期（UTC）。Iceberg 按天分区的分区键。 */
    private LocalDate eventDate;

    /** 事件类型标识。用于 Kafka 消费端区分不同事件。 */
    private String eventType;

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public void setEventTime(Instant eventTime) {
        this.eventTime = eventTime;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public LocalDate getEventDate() {
        return eventDate;
    }

    public void setEventDate(LocalDate eventDate) {
        this.eventDate = eventDate;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
}
