package com.pipeline.flink.kafka.enrichment;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pipeline.flink.common.JdbcAsyncLookupFunction;
import com.pipeline.flink.kafka.model.dwd.EnrichedOrder;

/**
 * 用户维表 AsyncIO 关联。
 * <p>
 * 继承 {@link JdbcAsyncLookupFunction}，复用连接池、线程池、异步查询逻辑，
 * 本类只需定义 SQL、参数和结果映射。
 * <p>
 * <b>更优的生产级方案：Flink SQL Lookup Join</b>
 * <p>
 * 当前 DataStream AsyncIO 方式需要手动管理连接池、SQL、映射。生产环境推荐使用 Flink SQL
 * 的 Lookup Join，零 Java 代码、内置缓存、声明式配置：
 * <pre>{@code
 * CREATE TABLE base_users (
 *   user_id BIGINT,
 *   user_name STRING,
 *   user_level STRING,
 *   PRIMARY KEY (user_id) NOT ENFORCED
 * ) WITH (
 *   'connector' = 'jdbc',
 *   'url' = 'jdbc:postgresql://...',
 *   'table-name' = 'base.users',
 *   'username' = '...',
 *   'password' = '...',
 *   'lookup.cache.max-rows' = '1000',
 *   'lookup.cache.ttl' = '10s'
 * );
 *
 * SELECT o.*, u.user_name, u.user_level
 * FROM orders o
 * LEFT JOIN base_users FOR SYSTEM_TIME AS OF o.proc_time AS u
 * ON o.user_id = u.user_id;
 * }</pre>
 * 本类作为 DataStream API 的参考实现保留。
 */
public class UserAsyncIoJoin extends JdbcAsyncLookupFunction<EnrichedOrder, EnrichedOrder> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(UserAsyncIoJoin.class);

    /** 查询 base.users 的 SQL。 */
    private static final String QUERY_SQL =
            "SELECT user_id, user_name, user_level, age, city_code FROM base.users WHERE user_id = ?";

    @Override
    protected String getSql(EnrichedOrder input) {
        return QUERY_SQL;
    }

    @Override
    protected void setParameters(PreparedStatement stmt, EnrichedOrder input) throws SQLException {
        stmt.setLong(1, input.getUser_id());
    }

    @Override
    protected EnrichedOrder mapRow(ResultSet rs, EnrichedOrder input) throws SQLException {
        // 在输入对象上填充用户画像字段
        input.setUser_name(rs.getString("user_name"));
        input.setUser_level(rs.getString("user_level"));
        input.setAge(rs.getInt("age"));
        input.setCity_code(rs.getString("city_code"));
        return input;
    }

    @Override
    protected boolean shouldLookup(EnrichedOrder input) {
        // user_id 为空时跳过查询
        return input.getUser_id() != null;
    }
}
