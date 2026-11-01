package com.pipeline.flink.common.serde;

/**
 * 反序列化结果包装类。
 * <p>
 * 用于在 Source 阶段区分解析成功和失败的数据：
 * <ul>
 *   <li>解析成功：{@link #isSuccess()} = true，主流输出 {@link #getData()}</li>
 *   <li>解析失败：{@link #isSuccess()} = false，侧输出 {@link #getRawMessage()} 到 DLQ</li>
 * </ul>
 *
 * @param <T> 解析成功后的目标类型
 */
public class DeserializationResult<T> {

    private final boolean success;
    private final T data;
    private final String rawMessage;

    private DeserializationResult(boolean success, T data, String rawMessage) {
        this.success = success;
        this.data = data;
        this.rawMessage = rawMessage;
    }

    /**
     * 构建成功结果。
     */
    public static <T> DeserializationResult<T> success(T data) {
        return new DeserializationResult<>(true, data, null);
    }

    /**
     * 构建失败结果，保留原始消息用于 DLQ。
     */
    public static <T> DeserializationResult<T> failure(String rawMessage) {
        return new DeserializationResult<>(false, null, rawMessage);
    }

    public boolean isSuccess() {
        return success;
    }

    public T getData() {
        return data;
    }

    public String getRawMessage() {
        return rawMessage;
    }
}
