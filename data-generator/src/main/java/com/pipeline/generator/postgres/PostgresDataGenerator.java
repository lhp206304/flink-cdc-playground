package com.pipeline.generator.postgres;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PostgreSQL 数据生成器。
 *
 * <p>向 PostgreSQL 事务表（public schema）写入数据，模拟真实电商的数据库变更。
 * 支持 INSERT / UPDATE / DELETE 三种操作，比例可通过环境变量配置。</p>
 *
 * <p>生成策略（按表）：</p>
 * <ul>
 *   <li>orders：高频 INSERT（主流量），含多商品明细</li>
 *   <li>order_items：随 orders 一起 INSERT</li>
 *   <li>payments：随 orders 延迟 INSERT</li>
 * </ul>
 *
 * <p>注意：users、products 已移至 base schema 作为基础数据表，由初始化脚本灌入，
 * 本生成器只操作事务表，不写维表。</p>
 *
 * <p>环境变量：</p>
 * <ul>
 *   <li>POSTGRES_HOST / POSTGRES_PORT / POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DATABASE</li>
 *   <li>PG_GENERATOR_TPS：每秒事务数</li>
 *   <li>PG_GENERATOR_DURATION_SECONDS：运行时长，-1 持续</li>
 *   <li>PG_INSERT_RATIO / PG_UPDATE_RATIO / PG_DELETE_RATIO：操作比例</li>
 * </ul>
 */
public class PostgresDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresDataGenerator.class);

    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final String database;
    private final int tps;
    private final int durationSeconds;
    private final double insertRatio;
    private final double updateRatio;
    private final double deleteRatio;

    private final PgDataWriter writer;
    private final Random random = ThreadLocalRandom.current();
    private final AtomicLong totalInserts = new AtomicLong(0);
    private final AtomicLong totalUpdates = new AtomicLong(0);
    private final AtomicLong totalDeletes = new AtomicLong(0);

    // 维表 ID 范围：users 1~100，products 1~20（由初始化脚本灌入 dim 维表）
    // 生成器只读取这些 ID 来生成订单，不写维表
    private static final long MIN_USER_ID = 1L;
    private static final long MAX_USER_ID = 100L;
    private static final long MIN_PRODUCT_ID = 1L;
    private static final long MAX_PRODUCT_ID = 20L;

    // 地区编码：与 base.region 表的 region_code 一致（由初始化脚本灌入）
    // 生成订单时随机选一个，供 Flink Broadcast 关联地区维表
    private static final String[] REGION_CODES = {
            "us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"
    };

    // 已存在的订单 ID 池，用于 UPDATE / DELETE
    private final List<Long> existingOrderIds = new ArrayList<>();

    private static final String[] ORDER_STATUSES = {"pending", "confirmed", "shipped", "delivered", "cancelled"};
    private static final String[] PAY_METHODS = {"alipay", "wechat", "card"};
    private static final String[] PAY_STATUSES = {"success", "failed", "pending"};
    private static final String[] CATEGORIES = {"electronics", "clothing", "books", "home", "sports"};
    private static final String[] BRANDS = {"Apple", "Samsung", "Nike", "Adidas", "Sony"};
    private static final String[] ADDRESSES = {
            "123 Main St, New York, NY",
            "456 Oak Ave, Los Angeles, CA",
            "789 Pine Rd, Chicago, IL",
            "321 Elm St, Houston, TX"
    };

    public PostgresDataGenerator() {
        this.host = getRequiredEnv("POSTGRES_HOST");
        this.port = Integer.parseInt(getRequiredEnv("POSTGRES_PORT"));
        this.user = getRequiredEnv("POSTGRES_USER");
        this.password = getRequiredEnv("POSTGRES_PASSWORD");
        this.database = getRequiredEnv("POSTGRES_DATABASE");
        this.tps = Integer.parseInt(getRequiredEnv("PG_GENERATOR_TPS"));
        this.durationSeconds = Integer.parseInt(getRequiredEnv("PG_GENERATOR_DURATION_SECONDS"));
        this.insertRatio = Double.parseDouble(getRequiredEnv("PG_INSERT_RATIO"));
        this.updateRatio = Double.parseDouble(getRequiredEnv("PG_UPDATE_RATIO"));
        this.deleteRatio = Double.parseDouble(getRequiredEnv("PG_DELETE_RATIO"));

        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
        this.writer = new PgDataWriter(jdbcUrl, user, password);
    }

    public void run() {
        LOG.info("===== PostgreSQL 数据生成器启动 =====");
        LOG.info("PG: {}:{}/{}", host, port, database);
        LOG.info("TPS: {}, 操作比例: INSERT={}, UPDATE={}, DELETE={}",
                tps, insertRatio, updateRatio, deleteRatio);

        try {
            // 基础表数据（base.users、base.products）已由初始化脚本灌入，无需再插入
            LOG.info("维表数据由初始化脚本提供，跳过基础数据插入");

            // 主循环：按 TPS 速率生成事务数据（orders + order_items + payments）
            long startTime = System.currentTimeMillis();
            long lastLogTime = startTime;

            while (shouldContinue(startTime)) {
                long batchStart = System.currentTimeMillis();

                generateBatch();

                if (System.currentTimeMillis() - lastLogTime >= 5000) {
                    LOG.info("进度: INSERT={}, UPDATE={}, DELETE={}",
                            totalInserts.get(), totalUpdates.get(), totalDeletes.get());
                    lastLogTime = System.currentTimeMillis();
                }

                long batchDuration = System.currentTimeMillis() - batchStart;
                long targetDuration = 1000;
                if (batchDuration < targetDuration) {
                    Thread.sleep(targetDuration - batchDuration);
                }
            }

            LOG.info("===== PostgreSQL 数据生成器结束 =====");
            LOG.info("总计: INSERT={}, UPDATE={}, DELETE={}",
                    totalInserts.get(), totalUpdates.get(), totalDeletes.get());

        } catch (InterruptedException e) {
            LOG.warn("生成器被中断", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.error("生成器异常", e);
            throw new RuntimeException("生成器失败", e);
        } finally {
            writer.close();
        }
    }

    /**
     * 生成一批数据（按操作比例分发）。
     */
    private void generateBatch() {
        int count = Math.min(tps, 500);
        for (int i = 0; i < count; i++) {
            double r = random.nextDouble();
            if (r < insertRatio) {
                doInsert();
            } else if (r < insertRatio + updateRatio) {
                doUpdate();
            } else {
                doDelete();
            }
        }
    }

    /**
     * INSERT：主要生成 orders + order_items，偶尔生成 payments。
     */
    private void doInsert() {
        // 从维表 ID 范围随机选一个用户
        Long userId = MIN_USER_ID + (long) random.nextInt((int) (MAX_USER_ID - MIN_USER_ID + 1));

        // 一单 1~3 个商品
        int itemCount = 1 + random.nextInt(3);
        BigDecimal totalAmount = BigDecimal.ZERO;
        BigDecimal discountAmount = BigDecimal.ZERO;

        List<Object[]> itemParams = new ArrayList<>();
        // 先生成明细参数（占位 orderId，插入订单后回填）
        List<Object[]> itemTempParams = new ArrayList<>();
        for (int j = 0; j < itemCount; j++) {
            Long productId = MIN_PRODUCT_ID + (long) random.nextInt((int) (MAX_PRODUCT_ID - MIN_PRODUCT_ID + 1));
            int quantity = 1 + random.nextInt(5);
            BigDecimal price = BigDecimal.valueOf(10 + random.nextDouble() * 990).setScale(2, RoundingMode.HALF_UP);
            BigDecimal itemDiscount = price.multiply(BigDecimal.valueOf(random.nextDouble() * 0.2))
                    .setScale(2, RoundingMode.HALF_UP);
            totalAmount = totalAmount.add(price.multiply(BigDecimal.valueOf(quantity)));
            discountAmount = discountAmount.add(itemDiscount);
            itemTempParams.add(new Object[]{null, productId, "product_" + productId, price, quantity, price, itemDiscount});
        }

        // 插入订单（返回自增 order_id 由数据库生成，这里用时间戳+随机数模拟，实际应取 getGeneratedKeys）
        long orderId = System.currentTimeMillis() * 1000 + random.nextInt(1000);
        String regionCode = REGION_CODES[random.nextInt(REGION_CODES.length)];

        String orderSql = "INSERT INTO orders (order_id, user_id, region_code, total_amount, discount_amount, payment_method, shipping_address, status) " +
                "VALUES (?,?,?,?,?,?,?,?)";
        writer.batchExecute(orderSql, Collections.singletonList(new Object[]{
                orderId, userId, regionCode, totalAmount, discountAmount,
                PAY_METHODS[random.nextInt(PAY_METHODS.length)],
                ADDRESSES[random.nextInt(ADDRESSES.length)],
                ORDER_STATUSES[0]
        }));

        // 回填 orderId 并插入明细
        for (Object[] row : itemTempParams) {
            row[0] = orderId;
            itemParams.add(row);
        }
        String itemSql = "INSERT INTO order_items (order_id, product_id, product_name, product_price, quantity, price, discount) " +
                "VALUES (?,?,?,?,?,?,?)";
        writer.batchExecute(itemSql, itemParams);

        existingOrderIds.add(orderId);
        totalInserts.addAndGet(itemCount + 1);

        // 30% 概率同时生成支付记录
        if (random.nextDouble() < 0.3) {
            String paySql = "INSERT INTO payments (order_id, amount, pay_method, status) VALUES (?,?,?,?)";
            writer.batchExecute(paySql, Collections.singletonList(new Object[]{
                    orderId, totalAmount,
                    PAY_METHODS[random.nextInt(PAY_METHODS.length)],
                    PAY_STATUSES[random.nextInt(PAY_STATUSES.length)]
            }));
            totalInserts.incrementAndGet();
        }
    }

    /**
     * UPDATE：更新订单状态（维表不通过生成器更新，由业务系统维护）。
     */
    private void doUpdate() {
        if (existingOrderIds.isEmpty()) return;
        Long orderId = existingOrderIds.get(random.nextInt(existingOrderIds.size()));
        String sql = "UPDATE orders SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE order_id = ?";
        writer.batchExecute(sql, Collections.singletonList(new Object[]{
                ORDER_STATUSES[random.nextInt(ORDER_STATUSES.length)], orderId
        }));
        totalUpdates.incrementAndGet();
    }

    /**
     * DELETE：删除订单（测试 CDC DELETE）。
     */
    private void doDelete() {
        if (existingOrderIds.isEmpty()) return;
        int idx = random.nextInt(existingOrderIds.size());
        Long orderId = existingOrderIds.remove(idx);

        // 先删明细，再删订单
        String itemSql = "DELETE FROM order_items WHERE order_id = ?";
        writer.batchExecute(itemSql, Collections.singletonList(new Object[]{orderId}));

        String orderSql = "DELETE FROM orders WHERE order_id = ?";
        writer.batchExecute(orderSql, Collections.singletonList(new Object[]{orderId}));

        totalDeletes.addAndGet(2);
    }

    private boolean shouldContinue(long startTime) {
        if (durationSeconds < 0) {
            return true;
        }
        long elapsed = (System.currentTimeMillis() - startTime) / 1000;
        return elapsed < durationSeconds;
    }

    private static String getRequiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("环境变量 " + name + " 未设置，无法启动");
        }
        return value.trim();
    }

    public static void main(String[] args) {
        new PostgresDataGenerator().run();
    }
}
