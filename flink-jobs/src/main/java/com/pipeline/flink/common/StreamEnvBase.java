package com.pipeline.flink.common;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.sink.FlinkWriteResult;

import java.time.Duration;

/**
 * Flink 执行环境基础配置
 * <p>
 * 统一配置所有 Flink 作业的公共参数：
 * - Checkpoint（3s 间隔，精确一次，RocksDB 增量）
 * - 重启策略（固定延迟，最多 3 次）
 * - 状态后端（RocksDB）
 * <p>
 * 所有参数通过环境变量注入，无硬编码。
 */
public class StreamEnvBase {

    /**
     * 创建并配置好的 StreamExecutionEnvironment
     */
    public static StreamExecutionEnvironment createEnv() {
        Configuration conf = new Configuration();

        // 类加载顺序：parent-first（先从父加载器找 Flink 核心类，再从用户代码找）
        // 避免 MiniCluster 反序列化 StreamGraph 时找不到 flink-runtime 中的核心类
        conf.setString("classloader.resolve-order", "parent-first");

        // ===== Checkpoint 配置（满足 Kafka → Iceberg < 5 秒可见延迟） =====
        // 间隔：3 秒（决定 Iceberg 数据可见延迟）
        conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL,
                Duration.ofMillis(Long.parseLong(EnvConfig.getOptional("CHECKPOINT_INTERVAL_MS", "3000"))));
        // 单次 Checkpoint 超时：60 秒
        conf.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT,
                Duration.ofMillis(Long.parseLong(EnvConfig.getOptional("CHECKPOINT_TIMEOUT_MS", "60000"))));
        // 两次 Checkpoint 最小间隔：1 秒（防止重叠堆积）
        conf.set(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS,
                Duration.ofMillis(Long.parseLong(EnvConfig.getOptional("CHECKPOINT_MIN_PAUSE_MS", "1000"))));
        // 同一时间只允许一个 Checkpoint 在跑
        conf.set(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, 1);
        // 语义：精确一次（数据不丢不重）
        conf.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE, CheckpointingMode.EXACTLY_ONCE);
        // 作业取消时保留 Checkpoint（便于从 Checkpoint 恢复）
        conf.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        // ===== 状态后端：RocksDB + 增量 Checkpoint =====
        // 状态存磁盘，支持大状态；增量 Checkpoint 只上传变化部分，减少快照数据量
        conf.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        conf.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);

        // ===== 重启策略：固定延迟，最多 3 次，间隔 10 秒 =====
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS,
                Integer.parseInt(EnvConfig.getOptional("RESTART_ATTEMPTS", "3")));
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                Duration.ofSeconds(Long.parseLong(EnvConfig.getOptional("RESTART_DELAY_SECONDS", "10"))));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 允许连续失败的 Checkpoint 次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(
                Integer.parseInt(EnvConfig.getOptional("CHECKPOINT_TOLERABLE_FAILURES", "3")));

        // ===== 注册 Iceberg FlinkWriteResult 的 Kryo 序列化器 =====
        // FlinkWriteResult 内部 WriteResult.dataFiles 含不可变 Map（lowerBounds），
        // Kryo 默认序列化器会抛出 NullPointerException，需用 Iceberg 官方 WriteResultSerializer 包装。
        ((SerializerConfigImpl) env.getConfig().getSerializerConfig()).registerTypeWithKryoSerializer(
                FlinkWriteResult.class, FlinkWriteResultKryoSerializer.class);

        return env;
    }
}
