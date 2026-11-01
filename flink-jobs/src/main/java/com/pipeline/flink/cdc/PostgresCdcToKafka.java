package com.pipeline.flink.cdc;

import org.apache.flink.cdc.cli.parser.YamlPipelineDefinitionParser;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pipeline.flink.common.EnvConfig;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 链路 1：PostgreSQL CDC → Kafka
 * <p>
 * 使用 Flink CDC 3.x Pipeline API（YAML 声明式）实现 Postgres 到 Kafka 的整库实时同步。
 * <p>
 * 调用链：
 * 1. 从 classpath 加载 YAML
 * 2. 替换 ${ENV_VAR} 占位符
 * 3. YamlPipelineDefinitionParser 解析为 PipelineDef
 * 4. FlinkPipelineComposer 组装为 PipelineExecution
 * 5. execute() 提交作业（阻塞运行）
 * <p>
 * Pipeline 定义见 resources/pipelines/postgres-to-kafka.yaml
 */
public class PostgresCdcToKafka {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresCdcToKafka.class);

    // YAML 在 classpath 中的路径
    private static final String YAML_PATH = "pipelines/postgres-to-kafka.yaml";

    // 匹配 ${VAR_NAME} 占位符
    private static final Pattern ENV_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");

    public static void main(String[] args) throws Exception {
        LOG.info("===== PostgresCdcToKafka 启动 =====");

        // 1. 加载 YAML
        String yamlContent = loadYaml(YAML_PATH);
        LOG.info("已加载 YAML: {}", YAML_PATH);

        // 2. 替换环境变量占位符
        yamlContent = replaceEnvVars(yamlContent);
        LOG.info("环境变量占位符替换完成");

        // 3. 解析 YAML → PipelineDef
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(yamlContent, new Configuration());
        LOG.info("PipelineDef 解析成功");

        // 4. 组装为可执行的 Flink 作业
        // 部署模式由环境变量 FLINK_DEPLOY_TARGET 控制：local（默认）/ remote
        String target = EnvConfig.getOptional("FLINK_DEPLOY_TARGET", "local");
        // 需要随作业提交到集群的额外 jar 包（本地 MiniCluster 不需要）
        List<Path> additionalJars = Collections.emptyList();

        FlinkPipelineComposer composer;
        if ("remote".equals(target)) {
            // 远程集群：需要指定 JobManager 地址
            org.apache.flink.configuration.Configuration flinkConf =
                    new org.apache.flink.configuration.Configuration();
            // rest.address：JobManager REST API 的主机地址（即 Flink Web UI 的地址）
            // 客户端通过 REST 接口向远程集群提交作业
            flinkConf.setString("rest.address", EnvConfig.getRequired("FLINK_JOBMANAGER_HOST"));
            // rest.port：JobManager REST API 的端口，默认 8081
            // 与 rest.address 共同组成完整的提交地址，如 http://host:8081
            flinkConf.setString("rest.port",
                    EnvConfig.getOptional("FLINK_JOBMANAGER_PORT", "8081"));
            composer = FlinkPipelineComposer.ofRemoteCluster(flinkConf, additionalJars);
            LOG.info("部署模式: remote -> {}:{}",
                    flinkConf.getString("rest.address", ""),
                    flinkConf.getString("rest.port", "8081"));
        } else {
            // 本地 MiniCluster：IDE 直接运行，进程内启动迷你 Flink 集群
            composer = FlinkPipelineComposer.ofMiniCluster();
            LOG.info("部署模式: local (MiniCluster)");
        }

        // 5. 提交并执行（流式作业，阻塞直到作业结束）
        PipelineExecution execution = composer.compose(pipelineDef);
        PipelineExecution.ExecutionInfo info = execution.execute();

        LOG.info("作业执行完成: id={}, desc={}", info.getId(), info.getDescription());
    }

    /**
     * 从 classpath 加载 YAML 文件为字符串。
     */
    private static String loadYaml(String resourcePath) throws Exception {
        try (InputStream is = PostgresCdcToKafka.class.getClassLoader()
                .getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalStateException("Pipeline YAML 不存在: " + resourcePath);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    // Flink CDC 内置占位符（非环境变量，替换时保留原样）
    // ${table}：Kafka sink 中用于按表名路由 topic
    private static final Set<String> CDC_PLACEHOLDERS = Set.of("table");

    /**
     * 将 YAML 中的 ${ENV_VAR} 替换为实际环境变量值。
     * 缺失的环境变量直接抛异常（无默认值原则）。
     * Flink CDC 内置占位符（如 ${table}）保留原样。
     */
    private static String replaceEnvVars(String content) {
        Matcher matcher = ENV_PATTERN.matcher(content);
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String envKey = matcher.group(1);
            // Flink CDC 内置占位符保留原样
            if (CDC_PLACEHOLDERS.contains(envKey)) {
                matcher.appendReplacement(result, Matcher.quoteReplacement(matcher.group()));
                continue;
            }
            String envValue = System.getenv(envKey);
            if (envValue == null || envValue.isBlank()) {
                throw new IllegalStateException("必填环境变量未设置: " + envKey);
            }
            // quoteReplacement 防止值中含 $、\ 时被当成特殊字符
            matcher.appendReplacement(result, Matcher.quoteReplacement(envValue));
        }
        matcher.appendTail(result);
        return result.toString();
    }
}
