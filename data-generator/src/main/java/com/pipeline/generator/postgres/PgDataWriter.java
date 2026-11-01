package com.pipeline.generator.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * PostgreSQL 数据写入器。
 *
 * <p>封装 JDBC Batch 写入逻辑，供 PostgresDataGenerator 调用。
 * 内部使用 HikariCP 连接池管理连接，使用 PreparedStatement + addBatch + executeBatch
 * 实现批量写入，减少网络往返，提高吞吐。</p>
 */
public class PgDataWriter {

    private static final Logger LOG = LoggerFactory.getLogger(PgDataWriter.class);

    private final HikariDataSource dataSource;

    public PgDataWriter(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000);
        // 真正批量执行
        config.addDataSourceProperty("reWriteBatchedInserts", "true");
        this.dataSource = new HikariDataSource(config);
        LOG.info("HikariCP 连接池初始化成功: {}", jdbcUrl);
    }

    /**
     * 批量执行 SQL（INSERT / UPDATE / DELETE）。
     *
     * @param sql    SQL 模板，使用 ? 占位符
     * @param params 批量参数列表，每个 Object[] 对应一行
     */
    public void batchExecute(String sql, List<Object[]> params) {
        if (params == null || params.isEmpty()) {
            return;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (Object[] row : params) {
                for (int i = 0; i < row.length; i++) {
                    ps.setObject(i + 1, row[i]);
                }
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            LOG.error("批量执行失败: {}", sql, e);
            throw new RuntimeException("批量执行失败", e);
        }
    }

    /**
     * 关闭连接池。
     */
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            LOG.info("HikariCP 连接池已关闭");
        }
    }
}
