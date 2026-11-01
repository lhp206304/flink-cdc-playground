# Iceberg 建表 DDL

> 本目录用于说明 Iceberg 数据湖的表结构设计。
> Iceberg 表由 Flink 作业通过 Table API **自动创建**（表不存在时 `CREATE TABLE`），无需手动执行 DDL。
> 建表逻辑见 `flink-jobs/.../common/IcebergSinkFactory.java`。

## Catalog 配置

| 配置项 | 环境变量 | 默认值 | 说明 |
|--------|---------|--------|------|
| Catalog 名称 | `ICEBERG_CATALOG_NAME` | local_catalog | Catalog 标识 |
| Warehouse 路径 | `ICEBERG_WAREHOUSE_PATH` | ./warehouse | 数据存储根目录 |
| 默认数据库 | `ICEBERG_DEFAULT_DATABASE` | dwd | 默认 namespace |

**Catalog 类型：** HadoopCatalog（本地文件系统，适合开发/练习）

**数据位置：** `./warehouse/<database>/<table>/`
- `data/` — parquet 数据文件
- `metadata/` — 快照元数据

---

## 表清单

| 数据库 | 表名 | 写入模式 | 说明 |
|--------|------|---------|------|
| dwd | order_payment_wide | APPEND | 订单+支付+维表富化宽表 |

> **注：** 当前项目只实现了 DWD 层宽表。ODS 层单表（ods.orders 等）作为可选扩展，见 `docs/03-开发任务清单.md`。

---

## dwd.order_payment_wide — 订单支付宽表

订单流 + 支付流按 `order_id` JOIN，再关联 region（Broadcast）和 users（AsyncIO）维表后的宽表。

### 字段定义

| 字段 | 类型 | 来源 | 说明 |
|------|------|------|------|
| order_id | BIGINT | orders | 订单 ID |
| user_id | BIGINT | orders | 用户 ID |
| region_code | STRING | orders | 地区编码 |
| total_amount | DECIMAL(12,2) | orders | 订单总金额 |
| discount_amount | DECIMAL(10,2) | orders | 优惠金额 |
| order_status | STRING | orders | 订单状态 |
| order_created_at | TIMESTAMP(6) | orders | 订单创建时间 |
| payment_id | BIGINT | payments | 支付 ID |
| payment_amount | DECIMAL(12,2) | payments | 支付金额 |
| pay_method | STRING | payments | 支付方式 |
| payment_status | STRING | payments | 支付状态 |
| payment_created_at | TIMESTAMP(6) | payments | 支付创建时间 |
| region_name | STRING | base.region（Broadcast） | 地区名称 |
| user_name | STRING | base.users（AsyncIO） | 用户名 |
| user_level | STRING | base.users（AsyncIO） | 用户等级 |

### 建表方式

由 `IcebergSinkFactory.writeToIceberg()` 根据传入的 `Schema` 自动生成 `CREATE TABLE`：

```sql
CREATE TABLE `order_payment_wide` (
  `order_id` BIGINT,
  `user_id` BIGINT,
  `region_code` STRING,
  `total_amount` DECIMAL(12, 2),
  `discount_amount` DECIMAL(10, 2),
  `order_status` STRING,
  `order_created_at` TIMESTAMP(6),
  `payment_id` BIGINT,
  `payment_amount` DECIMAL(12, 2),
  `pay_method` STRING,
  `payment_status` STRING,
  `payment_created_at` TIMESTAMP(6),
  `region_name` STRING,
  `user_name` STRING,
  `user_level` STRING
)
```

### Schema 定义位置

`flink-jobs/src/main/java/com/pipeline/flink/kafka/DwdOrderPaymentJob.java` 中的 `buildIcebergSchema()` 方法。

---

## 数据查询

Iceberg 数据以 parquet 文件存储，可用以下方式查询：

### Python（pyarrow）

```python
import pyarrow.parquet as pq
import glob
import pyarrow as pa

t = pa.concat_tables([pq.read_table(f) for f in glob.glob('warehouse/dwd/order_payment_wide/data/*.parquet')])
print(f"总记录数: {t.num_rows}")
print(t.to_pandas().head())
```

### Spark SQL

```sql
-- 配置 Iceberg Catalog
CREATE CATALOG iceberg USING iceberg OPTIONS (
  'type' = 'hadoop',
  'warehouse' = './warehouse'
);

-- 查询
SELECT count(*) FROM iceberg.dwd.order_payment_wide;
```

---

## 表属性

当前使用 Iceberg 默认表属性，未显式配置分区、排序等。

| 属性 | 当前值 | 说明 |
|------|--------|------|
| format-version | 2（默认） | Iceberg v2 表格式 |
| 分区 | 无 | 全表扫描（数据量小，适合练习） |
| 写入模式 | APPEND | 追加写入 |

生产环境建议：
- 按日期分区（如 `dt` 字段）
- 配置 `write.distribution-mode` 优化写入
- 启用 Compaction 合并小文件
