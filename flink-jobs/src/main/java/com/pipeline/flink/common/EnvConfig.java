package com.pipeline.flink.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 环境变量配置统一读取类
 * <p>
 * 设计原则：
 * 1. 所有连接信息（主机、端口、用户、密码）从环境变量获取
 * 2. 无回退默认值，缺失则立即抛出异常（启动失败）
 * 3. 作业启动时输出最终生效的配置快照，便于排查
 */
public class EnvConfig {

    private static final Logger LOG = LoggerFactory.getLogger(EnvConfig.class);

    private EnvConfig() {
    }

    /**
     * 读取必填环境变量，缺失则抛出异常
     *
     * @param key 环境变量名
     * @return 环境变量值
     * @throws IllegalStateException 环境变量未设置
     */
    public static String getRequired(String key) {
        String value = System.getenv(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException(
                    String.format("必填环境变量未设置: %s", key));
        }
        return value.trim();
    }

    /**
     * 读取可选环境变量，缺失返回默认值
     *
     * @param key          环境变量名
     * @param defaultValue 默认值
     * @return 环境变量值或默认值
     */
    public static String getOptional(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        return value.trim();
    }
}
