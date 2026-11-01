package com.pipeline.flink.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * CDC 变更的元信息。
 * <p>
 * 对应 Debezium JSON 中的 source 字段，记录变更来自哪个库、哪张表、WAL 位置等。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CdcSource {

    /** 数据库名。 */
    private String db;

    /** 表名。 */
    private String table;

    /** PostgreSQL WAL 位置（Log Sequence Number）。 */
    private Long lsn;

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Long getLsn() {
        return lsn;
    }

    public void setLsn(Long lsn) {
        this.lsn = lsn;
    }
}
