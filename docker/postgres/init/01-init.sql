-- ============================================================
-- PostgreSQL 初始化脚本
-- 1. 创建 base schema（基础数据表，不走 CDC）
-- 2. 创建业务表（public schema，事务表，走 CDC）
-- 3. 创建基础数据表（base schema，不走 CDC，Broadcast/AsyncIO 查）
-- 4. 创建 CDC 专用账号
-- 5. 维表初始数据
-- ============================================================

-- 创建 base schema（业务基础数据表，不走 CDC）
CREATE SCHEMA IF NOT EXISTS base;

-- ============================================================
-- 业务表（public schema）—— 事务型实体，走 CDC 同步到 Kafka
-- ============================================================

-- 订单表
CREATE TABLE IF NOT EXISTS public.orders (
    order_id         BIGSERIAL PRIMARY KEY,
    user_id          BIGINT NOT NULL,
    region_code      VARCHAR(20),
    total_amount     DECIMAL(12, 2),
    discount_amount  DECIMAL(10, 2) DEFAULT 0,
    coupon_id        BIGINT,
    payment_method   VARCHAR(20),
    shipping_address VARCHAR(500),
    status           VARCHAR(20),
    remark           VARCHAR(500),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 订单明细表（下单时快照商品信息）
CREATE TABLE IF NOT EXISTS public.order_items (
    item_id       BIGSERIAL PRIMARY KEY,
    order_id      BIGINT NOT NULL,
    product_id    BIGINT NOT NULL,
    product_name  VARCHAR(200),
    product_price DECIMAL(10, 2),
    quantity      INT,
    price         DECIMAL(10, 2),
    discount      DECIMAL(10, 2) DEFAULT 0,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 支付表
CREATE TABLE IF NOT EXISTS public.payments (
    payment_id    BIGSERIAL PRIMARY KEY,
    order_id      BIGINT NOT NULL,
    amount        DECIMAL(12, 2),
    pay_method    VARCHAR(20),
    status        VARCHAR(20),
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 基础数据表（base schema）—— 慢变数据，不走 CDC，Flink 直接查 PG
-- ============================================================

-- 地区表（小表，Broadcast 广播流用）
CREATE TABLE IF NOT EXISTS base.region (
    region_code   VARCHAR(20) PRIMARY KEY,
    region_name   VARCHAR(100) NOT NULL,
    province      VARCHAR(100),
    city          VARCHAR(100),
    country       VARCHAR(50),
    timezone      VARCHAR(50),
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 用户表（中表，AsyncIO + JDBC 用）
-- 合并了用户基础字段 + 用户画像字段，用户信息只存一份
CREATE TABLE IF NOT EXISTS base.users (
    user_id       BIGINT PRIMARY KEY,
    user_name     VARCHAR(100) NOT NULL,
    email         VARCHAR(200),
    phone         VARCHAR(20),
    city_code     VARCHAR(20),
    gender        SMALLINT,
    birthday      DATE,
    member_level  INT DEFAULT 1,
    user_level    VARCHAR(20),
    age           INT,
    register_date DATE,
    total_orders  INT DEFAULT 0,
    total_amount  DECIMAL(14, 2) DEFAULT 0,
    status        SMALLINT DEFAULT 1,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 商品表（中表，AsyncIO + JDBC 用）
-- 商品信息只存一份，不走 CDC，价格由业务系统直接更新
CREATE TABLE IF NOT EXISTS base.products (
    product_id    BIGINT PRIMARY KEY,
    product_name  VARCHAR(200) NOT NULL,
    category      VARCHAR(100),
    brand         VARCHAR(100),
    price         DECIMAL(10, 2),
    weight        DECIMAL(8, 2),
    stock         INT DEFAULT 0,
    status        SMALLINT DEFAULT 1,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- CDC 专用账号（同时用于基础表查询，已有 base schema SELECT 权限）
-- ============================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'cdc_user') THEN
        CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'cdc_pass';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT USAGE ON SCHEMA base TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA base TO cdc_user;
-- 未来 base schema 新建的表也自动授权
ALTER DEFAULT PRIVILEGES IN SCHEMA base GRANT SELECT ON TABLES TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

-- ============================================================
-- 维表初始数据
-- ============================================================

-- 地区表初始数据
INSERT INTO base.region (region_code, region_name, province, city, country, timezone) VALUES
    ('us-east-1', '美国东部', '弗吉尼亚', '阿什本', '美国', 'UTC-5'),
    ('us-west-2', '美国西部', '俄勒冈', '波特兰', '美国', 'UTC-8'),
    ('eu-west-1', '欧洲西部', '爱尔兰', '都柏林', '爱尔兰', 'UTC+0'),
    ('ap-southeast-1', '亚太东南', '新加坡', '新加坡', '新加坡', 'UTC+8');

-- 用户表初始数据：100 个用户，供 orders 表关联
INSERT INTO base.users (user_id, user_name, email, phone, city_code, gender, member_level, user_level, age, register_date)
SELECT
    i,
    'user_' || i,
    'user_' || i || '@example.com',
    '138' || lpad(i::text, 8, '0'),
    'city_' || (i % 10),
    i % 2,
    1 + (i % 5),
    CASE WHEN i <= 10 THEN 'gold' WHEN i <= 50 THEN 'silver' ELSE 'bronze' END,
    18 + (i % 40),
    CURRENT_DATE - (i || ' days')::interval
FROM generate_series(1, 100) AS i;

-- 商品表初始数据：20 个商品，供 order_items 关联
INSERT INTO base.products (product_id, product_name, category, brand, price, weight, stock) VALUES
    (1, 'iPhone 15', 'electronics', 'Apple', 999.00, 0.17, 500),
    (2, 'MacBook Air', 'electronics', 'Apple', 1299.00, 1.24, 200),
    (3, 'AirPods Pro', 'electronics', 'Apple', 249.00, 0.05, 1000),
    (4, 'iPad Mini', 'electronics', 'Apple', 499.00, 0.29, 300),
    (5, 'T-Shirt', 'clothing', 'Nike', 29.00, 0.20, 2000),
    (6, 'Jeans', 'clothing', 'Levi''s', 79.00, 0.50, 800),
    (7, 'Jacket', 'clothing', 'Adidas', 159.00, 0.80, 400),
    (8, 'Sneakers', 'clothing', 'Nike', 129.00, 0.60, 600),
    (9, 'Novel', 'books', 'Penguin', 19.00, 0.30, 3000),
    (10, 'Textbook', 'books', 'Pearson', 89.00, 1.50, 500),
    (11, 'Comics', 'books', 'Marvel', 12.00, 0.20, 1500),
    (12, 'Cookbook', 'books', 'DK', 35.00, 0.80, 700),
    (13, 'Coffee Maker', 'home', 'Philips', 89.00, 2.50, 350),
    (14, 'Desk Lamp', 'home', 'IKEA', 45.00, 1.20, 900),
    (15, 'Vacuum Cleaner', 'home', 'Dyson', 299.00, 3.50, 150),
    (16, 'Air Fryer', 'home', 'Philips', 119.00, 4.00, 250),
    (17, 'Yoga Mat', 'sports', 'Adidas', 39.00, 1.00, 1200),
    (18, 'Dumbbells', 'sports', 'Nike', 69.00, 5.00, 400),
    (19, 'Bicycle', 'sports', 'Giant', 599.00, 12.00, 100),
    (20, 'Tennis Racket', 'sports', 'Wilson', 149.00, 0.30, 300);
