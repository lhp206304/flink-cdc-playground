package com.pipeline.flink.kafka;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.pipeline.flink.common.serde.DeserializationResult;
import com.pipeline.flink.kafka.model.CdcEnvelope;

/**
 * CDC 反序列化结果分流器。
 * <p>
 * 将 {@link DeserializationResult} 拆分为两路：
 * <ul>
 *   <li>解析成功 → 主流输出 {@link CdcEnvelope}（后续 JOIN/维表关联用）</li>
 *   <li>解析失败 → 侧输出原始 JSON 字符串（写入 DLQ topic）</li>
 * </ul>
 *
 * @param <T> 业务实体类型（如 Order、Payment）
 */
public class CdcDeserializationSplitter<T>
        extends ProcessFunction<DeserializationResult<CdcEnvelope<T>>, CdcEnvelope<T>> {

    private static final long serialVersionUID = 1L;

    /** DLQ 侧输出标签。 */
    private final OutputTag<String> dlqTag;

    public CdcDeserializationSplitter(OutputTag<String> dlqTag) {
        this.dlqTag = dlqTag;
    }

    @Override
    public void processElement(
            DeserializationResult<CdcEnvelope<T>> value,
            Context ctx,
            Collector<CdcEnvelope<T>> out) {
        if (value.isSuccess()) {
            // 解析成功 → 主流输出 CdcEnvelope
            out.collect(value.getData());
        } else {
            // 解析失败 → 侧输出原始 JSON 到 DLQ
            ctx.output(dlqTag, value.getRawMessage());
        }
    }
}
