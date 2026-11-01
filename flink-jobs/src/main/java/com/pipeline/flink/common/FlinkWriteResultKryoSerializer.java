package com.pipeline.flink.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.iceberg.flink.sink.FlinkWriteResult;
import org.apache.iceberg.flink.sink.WriteResultSerializer;
import org.apache.iceberg.io.WriteResult;

import java.io.IOException;

/**
 * FlinkWriteResult 的 Kryo 序列化器。
 * <p>
 * 背景：Iceberg 的 {@link FlinkWriteResult} 内部持有 {@link WriteResult}，
 * 而 WriteResult 中的 {@code dataFiles}（List&#60;DataFile&#62;）包含
 * {@code lowerBounds}（Map&#60;Integer, ByteBuffer&#62;）等不可变 Map 字段。
 * Kryo 默认序列化器对 {@code Collections.emptyMap()} 等不可变 Map 会抛出
 * {@code NullPointerException: this.wrapped is null}，导致 Iceberg committer
 * 在 Checkpoint 时序列化失败。
 * <p>
 * 解决方案：复用 Iceberg 官方的 {@link WriteResultSerializer}
 * （实现 Flink {@code SimpleVersionedSerializer}）来序列化内部 WriteResult，
 * 外层仅处理 checkpointId（long）字段。
 * <p>
 * 注册方式（在 StreamEnvBase.createEnv 中）：
 * <pre>
 * env.getConfig().registerTypeWithKryoSerializer(
 *     FlinkWriteResult.class, FlinkWriteResultKryoSerializer.class);
 * </pre>
 */
public class FlinkWriteResultKryoSerializer extends Serializer<FlinkWriteResult> {

    /** 复用 Iceberg 官方的 WriteResult 序列化器，处理 DataFile 的 lowerBounds 等复杂字段。 */
    private final WriteResultSerializer writeResultSerializer = new WriteResultSerializer();

    @Override
    public void write(Kryo kryo, Output output, FlinkWriteResult object) {
        // 1. 写入 checkpointId（long，8 字节）
        output.writeLong(object.checkpointId());

        // 2. 用 Iceberg 官方序列化器序列化 WriteResult，再以长度前缀方式写入
        try {
            byte[] bytes = writeResultSerializer.serialize(object.writeResult());
            output.writeInt(bytes.length);
            output.writeBytes(bytes);
        } catch (IOException e) {
            throw new RuntimeException("序列化 FlinkWriteResult.writeResult 失败", e);
        }
    }

    @Override
    public FlinkWriteResult read(Kryo kryo, Input input, Class<? extends FlinkWriteResult> type) {
        // 1. 读取 checkpointId
        long checkpointId = input.readLong();

        // 2. 读取长度前缀的 WriteResult 字节数组，再用 Iceberg 官方序列化器反序列化
        int len = input.readInt();
        byte[] bytes = input.readBytes(len);
        try {
            WriteResult writeResult = writeResultSerializer.deserialize(
                    writeResultSerializer.getVersion(), bytes);
            return new FlinkWriteResult(checkpointId, writeResult);
        } catch (IOException e) {
            throw new RuntimeException("反序列化 FlinkWriteResult.writeResult 失败", e);
        }
    }
}
