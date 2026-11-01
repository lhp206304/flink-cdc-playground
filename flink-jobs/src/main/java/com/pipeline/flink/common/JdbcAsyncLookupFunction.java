package com.pipeline.flink.common;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * JDBC 异步查询维表的通用基类。
 * <p>
 * 封装 HikariCP 连接池、线程池、异步查询、超时处理等公共逻辑，
 * 子类只需实现 SQL 定义、参数设置和结果映射。
 * <p>
 * 适用场景：维表数据量较大，不宜全量广播到内存，采用异步查询数据库。
 *
 * @param <IN>  输入流元素类型
 * @param <OUT> 输出流元素类型
 */
public abstract class JdbcAsyncLookupFunction<IN, OUT> extends RichAsyncFunction<IN, OUT> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcAsyncLookupFunction.class);

    /** HikariCP 连接池（transient：不参与序列化，由 open 初始化）。 */
    private transient HikariDataSource dataSource;

    /** 异步查询线程池。 */
    private transient ExecutorService executor;

    @Override
    public void open(OpenContext openContext) throws Exception {
        // 从环境变量读取数据库配置，初始化 HikariCP 连接池
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:postgresql://%s:%s/%s",
                EnvConfig.getRequired("POSTGRES_HOST"),
                EnvConfig.getRequired("POSTGRES_PORT"),
                EnvConfig.getRequired("POSTGRES_DATABASE")));
        config.setUsername(EnvConfig.getRequired("POSTGRES_DIM_USER"));
        config.setPassword(EnvConfig.getRequired("POSTGRES_DIM_PASSWORD"));
        config.setMaximumPoolSize(3);
        config.setConnectionTimeout(5000);
        this.dataSource = new HikariDataSource(config);

        // 异步查询线程池，线程数与连接池大小一致
        this.executor = Executors.newFixedThreadPool(3);
    }

    @Override
    public void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) {
        // 子类判断是否需要查询（如关联 key 为空时跳过）
        if (!shouldLookup(input)) {
            resultFuture.complete(Collections.singletonList(noLookupResult(input)));
            return;
        }

        CompletableFuture.supplyAsync(() -> {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(getSql(input))) {
                // 子类设置查询参数
                setParameters(stmt, input);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return mapRow(rs, input);
                    }
                }
            } catch (Exception e) {
                LOG.warn("[AsyncIO] JDBC 异步查询失败: {}", e.getMessage());
            }
            return null;
        }, executor).thenAccept(result -> {
            // 查询成功但无数据，或查询异常时，返回未关联的原始结果
            if (result == null) {
                result = noLookupResult(input);
            }
            resultFuture.complete(Collections.singletonList(result));
        });
    }

    @Override
    public void timeout(IN input, ResultFuture<OUT> resultFuture) {
        // 超时降级：直接返回未关联的原始数据，不丢数据
        LOG.warn("JDBC 异步查询超时，降级返回原始数据");
        resultFuture.complete(Collections.singletonList(noLookupResult(input)));
    }

    @Override
    public void close() throws Exception {
        if (executor != null) {
            executor.shutdown();
        }
        if (dataSource != null) {
            dataSource.close();
        }
    }

    // ============================================================
    // 子类需实现的抽象方法
    // ============================================================

    /**
     * 返回查询 SQL。
     * <p>
     * 通常是固定的 SELECT 语句，也可以根据 input 动态生成。
     */
    protected abstract String getSql(IN input);

    /**
     * 设置 PreparedStatement 的查询参数。
     * <p>
     * 例如：stmt.setLong(1, input.getUser_id());
     */
    protected abstract void setParameters(PreparedStatement stmt, IN input) throws SQLException;

    /**
     * 将 ResultSet 的一行映射为输出对象。
     * <p>
     * 通常在 input 基础上填充维表字段后返回。
     */
    protected abstract OUT mapRow(ResultSet rs, IN input) throws SQLException;

    // ============================================================
    // 子类可选重写的方法
    // ============================================================

    /**
     * 是否需要执行查询。默认需要。
     * <p>
     * 子类可重写，例如关联 key 为空时返回 false 跳过查询。
     */
    protected boolean shouldLookup(IN input) {
        return true;
    }

    /**
     * 未命中或跳过时返回的结果。默认直接返回 input（需强转）。
     * <p>
     * 当 IN == OUT 时无需重写；否则子类必须重写。
     */
    @SuppressWarnings("unchecked")
    protected OUT noLookupResult(IN input) {
        return (OUT) input;
    }
}
