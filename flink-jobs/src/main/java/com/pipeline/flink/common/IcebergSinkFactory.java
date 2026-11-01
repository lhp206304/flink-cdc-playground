package com.pipeline.flink.common;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

/**
 * Iceberg 写入工具类（Table API 方式）。
 * <p>
 * 封装三步公共逻辑，避免每个作业重复编写：
 * <ol>
 *   <li>注册 Iceberg Catalog（HadoopCatalog）+ 默认数据库</li>
 *   <li>自动建表（表不存在时创建）</li>
 *   <li>DataStream → Table → INSERT INTO 写入</li>
 * </ol>
 * <p>
 * 配置项（环境变量，无默认值，必须显式配置）：
 * <ul>
 *   <li>{@code ICEBERG_CATALOG_NAME}：Catalog 名称</li>
 *   <li>{@code ICEBERG_WAREHOUSE_PATH}：warehouse 存储路径</li>
 *   <li>{@code ICEBERG_DEFAULT_DATABASE}：默认数据库名</li>
 * </ul>
 */
public class IcebergSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkFactory.class);

    /**
     * 创建 StreamTableEnvironment 并注册 Iceberg Catalog。
     * <p>
     * 同时创建默认数据库（IF NOT EXISTS）并切换到该数据库。
     * 每个作业只需调用一次，返回的 tableEnv 可复用于多个 sink。
     *
     * @param env 流执行环境
     * @return 已注册 Iceberg Catalog 并切换到默认数据库的表环境
     */
    public static StreamTableEnvironment createTableEnv(StreamExecutionEnvironment env) {
        String catalogName = EnvConfig.getRequired("ICEBERG_CATALOG_NAME");
        String warehouse = EnvConfig.getRequired("ICEBERG_WAREHOUSE_PATH");
        String defaultDatabase = EnvConfig.getRequired("ICEBERG_DEFAULT_DATABASE");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册 Iceberg Catalog（HadoopCatalog，本地文件系统）
        tableEnv.executeSql(
                "CREATE CATALOG `" + catalogName + "` WITH (" +
                        "'type' = 'iceberg', " +
                        "'catalog-type' = 'hadoop', " +
                        "'warehouse' = '" + warehouse + "'" +
                        ")"
        );
        tableEnv.useCatalog(catalogName);

        // 创建默认数据库（如果不存在）
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS `" + defaultDatabase + "`");
        tableEnv.useDatabase(defaultDatabase);

        LOG.info("Iceberg Catalog 已注册: catalog={}, warehouse={}, database={}",
                catalogName, warehouse, defaultDatabase);
        return tableEnv;
    }

    /**
     * 将 DataStream 写入指定的 Iceberg 表。
     * <p>
     * 执行顺序：
     * <ol>
     *   <li>检查表是否存在，不存在则用 TableDescriptor 自动建表</li>
     *   <li>DataStream 转为 Table</li>
     *   <li>INSERT INTO 写入 Iceberg</li>
     * </ol>
     *
     * @param tableEnv   已注册 Iceberg Catalog 的表环境（由 {@link #createTableEnv} 获取）
     * @param dataStream 待写入的数据流
     * @param schema     与 Iceberg 表字段名/类型对齐的 Schema（必须显式指定类型）
     * @param tableName  目标表名（不含库名，使用默认数据库）
     * @param <T>        数据流元素类型
     */
    public static <T> void writeToIceberg(
            StreamTableEnvironment tableEnv,
            DataStream<T> dataStream,
            Schema schema,
            String tableName) {

        String defaultDatabase = EnvConfig.getRequired("ICEBERG_DEFAULT_DATABASE");
        String catalogName = EnvConfig.getRequired("ICEBERG_CATALOG_NAME");

        // 2. DataStream → Table（先创建，用于解析列类型）
        Table table = tableEnv.fromDataStream(dataStream, schema);

        // 1. 自动建表（表不存在时创建）
        Catalog catalog = tableEnv.getCatalog(catalogName).orElseThrow(
                () -> new IllegalStateException("Iceberg Catalog 未注册: " + catalogName));
        ObjectPath tablePath = new ObjectPath(defaultDatabase, tableName);
        try {
            if (!catalog.tableExists(tablePath)) {
                // Iceberg Catalog 中建表：不能指定 'connector'='iceberg'（catalog 本身已决定）
                // 用 CREATE TABLE SQL 仅声明列定义，connector 由 catalog 隐式提供
                String columnsDdl = buildColumnsDdl(table.getResolvedSchema());
                String createSql = "CREATE TABLE `" + tableName + "` (" + columnsDdl + ")";
                tableEnv.executeSql(createSql);
                LOG.info("Iceberg 表已创建: {}.{}", defaultDatabase, tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException("建表失败: " + tablePath, e);
        }

        // 3. INSERT INTO 写入（提交并阻塞等待作业完成）
        // executeSql 返回 TableResult，调用 await() 阻塞直到流式作业结束
        // 注意：不能再调用 env.execute()，否则会提交第二个无 sink 的作业
        String insertSql = "INSERT INTO `" + tableName + "` SELECT * FROM " + table;
        LOG.info("执行写入 SQL: {}", insertSql);
        TableResult result = tableEnv.executeSql(insertSql);
        try {
            result.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Iceberg 写入作业被中断", e);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new RuntimeException("Iceberg 写入作业执行失败", e);
        }
    }

    /**
     * 将 {@link ResolvedSchema} 转换为 CREATE TABLE 的列定义 DDL 片段。
     * <p>
     * 例：返回 "`order_id` BIGINT, `amount` DECIMAL(10,2)"。
     *
     * @param resolved 已解析的表结构
     * @return 逗号分隔的列定义字符串
     */
    private static String buildColumnsDdl(ResolvedSchema resolved) {
        return resolved.getColumns().stream()
                .map(col -> "`" + col.getName() + "` " + col.getDataType().toString())
                .collect(Collectors.joining(", "));
    }
}
