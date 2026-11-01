# PostgreSQL 源库 + 维表建表 DDL

> 本目录用于说明 PostgreSQL 的表结构设计。
> 实际建表脚本位于 `docker/postgres/init/01-init.sql`，Docker 启动时自动执行。

## 表结构总览

| Schema | 表名 | 类型 | CDC | 用途 |
|--------|------|------|-----|------|
| public | orders | 业务表 | ✅ | 订单主表 |
| public | order_items | 业务表 | ✅ | 订单明细 |
| public | payments | 业务表 | ✅ | 支付记录 |
| base | region | 维表 | ✅ | 地区（小表，Broadcast） |
| base | users | 维表 | ❌ | 用户（中表，AsyncIO 直查 PG） |
| base | products | 维表 | ❌ | 商品（中表，AsyncIO 直查 PG） |

> **CDC 说明：** `public.*` 和 `base.region` 通过 Flink CDC 同步到 Kafka；`base.users` 和 `base.products` 不走 CDC，由 Flink AsyncIO 直接查 PG。

---

## 业务表（public schema）

### public.orders — 订单主表

| 字段 | 类型 | 说明 |
|------|------|------|
| order_id | BIGSERIAL PK | 订单 ID |
| user_id | BIGINT | 用户 ID |
| region_code | VARCHAR(20) | 地区编码（关联 base.region） |
| total_amount | DECIMAL(12,2) | 订单总金额 |
| discount_amount | DECIMAL(10,2) | 优惠金额 |
| coupon_id | BIGINT | 优惠券 ID |
| payment_method | VARCHAR(20) | 支付方式 |
| shipping_address | VARCHAR(500) | 收货地址 |
| status | VARCHAR(20) | 订单状态 |
| remark | VARCHAR(500) | 备注 |
| created_at | TIMESTAMP | 创建时间 |
| updated_at | TIMESTAMP | 更新时间 |

### public.order_items — 订单明细

| 字段 | 类型 | 说明 |
|------|------|------|
| item_id | BIGSERIAL PK | 明细 ID |
| order_id | BIGINT | 订单 ID（关联 orders） |
| product_id | BIGINT | 商品 ID（关联 base.products） |
| product_name | VARCHAR(200) | 下单时商品名快照 |
| product_price | DECIMAL(10,2) | 下单时商品价快照 |
| quantity | INT | 数量 |
| price | DECIMAL(10,2) | 单价 |
| discount | DECIMAL(10,2) | 折扣 |
| created_at | TIMESTAMP | 创建时间 |

### public.payments — 支付记录

| 字段 | 类型 | 说明 |
|------|------|------|
| payment_id | BIGSERIAL PK | 支付 ID |
| order_id | BIGINT | 订单 ID（关联 orders） |
| amount | DECIMAL(12,2) | 支付金额 |
| pay_method | VARCHAR(20) | 支付方式 |
| status | VARCHAR(20) | 支付状态 |
| created_at | TIMESTAMP | 创建时间 |
| updated_at | TIMESTAMP | 更新时间 |

---

## 维表（base schema）

### base.region — 地区表（小维表，Broadcast）

| 字段 | 类型 | 说明 |
|------|------|------|
| region_code | VARCHAR(20) PK | 地区编码 |
| region_name | VARCHAR(100) | 地区名称（如"美国东部"） |
| province | VARCHAR(100) | 省/州 |
| city | VARCHAR(100) | 城市 |
| country | VARCHAR(50) | 国家 |
| timezone | VARCHAR(50) | 时区 |
| updated_at | TIMESTAMP | 更新时间 |

初始数据：4 条（us-east-1、us-west-2、eu-west-1、ap-southeast-1）

### base.users — 用户表（中维表，AsyncIO）

| 字段 | 类型 | 说明 |
|------|------|------|
| user_id | BIGINT PK | 用户 ID |
| user_name | VARCHAR(100) | 用户名 |
| email | VARCHAR(200) | 邮箱 |
| phone | VARCHAR(20) | 电话 |
| city_code | VARCHAR(20) | 城市编码 |
| gender | SMALLINT | 性别 |
| birthday | DATE | 生日 |
| member_level | INT | 会员等级 |
| user_level | VARCHAR(20) | 用户等级（gold/silver/bronze） |
| age | INT | 年龄 |
| register_date | DATE | 注册日期 |
| total_orders | INT | 总订单数 |
| total_amount | DECIMAL(14,2) | 总消费金额 |
| status | SMALLINT | 状态 |
| created_at | TIMESTAMP | 创建时间 |
| updated_at | TIMESTAMP | 更新时间 |

初始数据：100 个用户（user_id 1~100）

### base.products — 商品表（中维表，AsyncIO）

| 字段 | 类型 | 说明 |
|------|------|------|
| product_id | BIGINT PK | 商品 ID |
| product_name | VARCHAR(200) | 商品名 |
| category | VARCHAR(100) | 分类 |
| brand | VARCHAR(100) | 品牌 |
| price | DECIMAL(10,2) | 价格 |
| weight | DECIMAL(8,2) | 重量 |
| stock | INT | 库存 |
| status | SMALLINT | 状态 |
| created_at | TIMESTAMP | 创建时间 |
| updated_at | TIMESTAMP | 更新时间 |

初始数据：20 个商品（product_id 1~20）

---

## CDC 账号

| 账号 | 密码 | 权限 |
|------|------|------|
| cdc_user | cdc_pass | REPLICATION + SELECT（public 和 base schema） |

```sql
CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'cdc_pass';
GRANT USAGE ON SCHEMA public, base TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public, base TO cdc_user;
```

---

## 手动执行建表

如果不用 Docker 自动初始化，可手动执行：

```bash
docker exec -i pg-cdc psql -U postgres -d ecommerce \
  < docker/postgres/init/01-init.sql
```
