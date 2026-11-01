package com.pipeline.flink.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Debezium CDC 信封（Envelope）。
 * <p>
 * Kafka 中 cdc.* topic 的每条消息都是这个结构：
 * <pre>
 * {
 *   "before": {...},   // 变更前的数据（INSERT 时为 null）
 *   "after":  {...},   // 变更后的数据（DELETE 时为 null）
 *   "op":     "c",     // 操作类型：c=insert, u=update, d=delete, r=snapshot
 *   "source": {...},   // 元信息（库名、表名、LSN）
 *   "ts_ms":  1698765432100  // 变更时间戳
 * }
 * </pre>
 *
 * @param <T> 实际业务实体类型（如 Order、Payment）
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CdcEnvelope<T> {

    /** 变更前的数据（INSERT 操作时为 null）。 */
    private T before;

    /** 变更后的数据（DELETE 操作时为 null）。 */
    private T after;

    /** 操作类型：c=create(insert), u=update, d=delete, r=read(snapshot)。 */
    private String op;

    /** 变更元信息（库名、表名、WAL 位置）。 */
    private CdcSource source;

    /** 变更发生的时间戳（毫秒）。 */
    private Long ts_ms;

    public T getBefore() {
        return before;
    }

    public void setBefore(T before) {
        this.before = before;
    }

    public T getAfter() {
        return after;
    }

    public void setAfter(T after) {
        this.after = after;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public CdcSource getSource() {
        return source;
    }

    public void setSource(CdcSource source) {
        this.source = source;
    }

    public Long getTs_ms() {
        return ts_ms;
    }

    public void setTs_ms(Long ts_ms) {
        this.ts_ms = ts_ms;
    }

    /**
     * 判断是否为删除操作。
     */
    public boolean isDelete() {
        return "d".equals(op);
    }

    /**
     * 判断是否为快照读（全量同步阶段产生）。
     */
    public boolean isSnapshot() {
        return "r".equals(op);
    }
}
