package com.pipeline.generator.kafka;

import com.pipeline.generator.model.BaseEvent;
import com.pipeline.generator.model.ClickEvent;
import com.pipeline.generator.model.ProductPriceEvent;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 事件工厂：按配置概率生成两类电商事件，并刻意注入"脏数据"用于练习。
 *
 * <p>核心设计：</p>
 * <ul>
 *   <li><b>事件类型分布</b>：点击 70% / 价格变更 30%</li>
 *   <li><b>重复主键</b>：从 ID 池中复用旧 eventId，用于练习 Iceberg upsert</li>
 *   <li><b>迟到事件</b>：eventTime 回退 1~60 分钟，用于练习 Watermark</li>
 *   <li><b>Schema 演化</b>：可选字段 scrollDepth 随机出现，用于练习 Iceberg schema evolution</li>
 *   <li><b>热点 region</b>：事件以指定概率集中写入热点 region，用于数据倾斜实验</li>
 * </ul>
 *
 * <p>事件类型说明：</p>
 * <ul>
 *   <li>CLICK：用户点击事件（浏览商品页面）</li>
 *   <li>PRODUCT_PRICE：商品价格变更事件（只含 productId + newPrice）</li>
 * </ul>
 *
 * <p>注意：订单数据走 PostgreSQL CDC（cdc.orders），不再生成订单事件流，避免数据重复。</p>
 */
public class EventFactory {

    // ==================== 取值域（固定常量池） ====================

    private static final String[] REGIONS = {"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"};
    private static final String HOT_REGION = REGIONS[0];
    private static final String[] REFERRERS = {"https://google.com", "https://example.com", "https://bing.com"};

    // ==================== 商品目录（固定维表数据） ====================

    /** 商品目录条目：商品号、名称、类目、基准价格。 */
    private record ProductInfo(String productId, String productName, String category, BigDecimal basePrice) {}

    /** 内置商品目录：固定 20 个商品，与 base.products 表数据对应。 */
    private static final List<ProductInfo> PRODUCT_CATALOG = List.of(
            new ProductInfo("1", "iPhone 15", "electronics", new BigDecimal("999.00")),
            new ProductInfo("2", "MacBook Air", "electronics", new BigDecimal("1299.00")),
            new ProductInfo("3", "AirPods Pro", "electronics", new BigDecimal("249.00")),
            new ProductInfo("4", "iPad Mini", "electronics", new BigDecimal("499.00")),
            new ProductInfo("5", "T-Shirt", "clothing", new BigDecimal("29.00")),
            new ProductInfo("6", "Jeans", "clothing", new BigDecimal("79.00")),
            new ProductInfo("7", "Jacket", "clothing", new BigDecimal("159.00")),
            new ProductInfo("8", "Sneakers", "clothing", new BigDecimal("129.00")),
            new ProductInfo("9", "Novel", "books", new BigDecimal("19.00")),
            new ProductInfo("10", "Textbook", "books", new BigDecimal("89.00")),
            new ProductInfo("11", "Comics", "books", new BigDecimal("12.00")),
            new ProductInfo("12", "Cookbook", "books", new BigDecimal("35.00")),
            new ProductInfo("13", "Coffee Maker", "home", new BigDecimal("89.00")),
            new ProductInfo("14", "Desk Lamp", "home", new BigDecimal("45.00")),
            new ProductInfo("15", "Vacuum Cleaner", "home", new BigDecimal("299.00")),
            new ProductInfo("16", "Air Fryer", "home", new BigDecimal("119.00")),
            new ProductInfo("17", "Yoga Mat", "sports", new BigDecimal("39.00")),
            new ProductInfo("18", "Dumbbells", "sports", new BigDecimal("69.00")),
            new ProductInfo("19", "Bicycle", "sports", new BigDecimal("599.00")),
            new ProductInfo("20", "Tennis Racket", "sports", new BigDecimal("149.00"))
    );

    // ==================== 配置项 ====================

    private final Random random;
    private final Map<String, Double> eventTypeDistribution;
    private final double duplicateKeyProbability;
    private final double lateArrivalProbability;
    private final double schemaEvolutionProbability;
    private final double hotRegionProbability;

    /** 每个事件类型的可复用 eventId 池。 */
    private final Map<String, Set<String>> reusableEventIdsByType;
    private final int maxReusableIdsPerType = 100;

    // ==================== 构造方法 ====================

    public EventFactory() {
        this(new HashMap<>(), 0.1, 0.05, 0.3, 0.0);
    }

    public EventFactory(Map<String, Double> eventTypeDistribution,
                        double duplicateKeyProbability,
                        double lateArrivalProbability,
                        double schemaEvolutionProbability,
                        double hotRegionProbability) {
        this.random = ThreadLocalRandom.current();

        // 事件分布：点击 70% / 价格变更 30%
        this.eventTypeDistribution = new HashMap<>(eventTypeDistribution);
        if (this.eventTypeDistribution.isEmpty()) {
            this.eventTypeDistribution.put("CLICK", 0.70);
            this.eventTypeDistribution.put("PRODUCT_PRICE", 0.30);
        }

        this.duplicateKeyProbability = clamp(duplicateKeyProbability);
        this.lateArrivalProbability = clamp(lateArrivalProbability);
        this.schemaEvolutionProbability = clamp(schemaEvolutionProbability);
        this.hotRegionProbability = clamp(hotRegionProbability);

        this.reusableEventIdsByType = new HashMap<>();
    }

    // ==================== 事件生成入口 ====================

    public BaseEvent createRandomEvent() {
        String eventType = selectEventType();
        return switch (eventType) {
            case "CLICK" -> createClickEvent();
            case "PRODUCT_PRICE" -> createProductPriceEvent();
            default -> throw new IllegalStateException("未知事件类型: " + eventType);
        };
    }

    // ==================== 点击事件 ====================

    public ClickEvent createClickEvent() {
        String eventId = generateEventId("CLICK");
        Instant eventTime = generateEventTime();
        String region = selectRegion();
        LocalDate eventDate = eventTime.atZone(ZoneOffset.UTC).toLocalDate();

        String sessionId = "SESSION-" + UUID.randomUUID().toString().substring(0, 12);
        String customerId = "CUST-" + random.nextInt(10000);
        ProductInfo product = randomProduct();
        String pageUrl = "https://example.com/products/" + product.productId();
        String referrer = randomElement(REFERRERS);

        ClickEvent event = new ClickEvent(eventId, eventTime, region, eventDate,
                sessionId, customerId, product.productId(), pageUrl, referrer);

        event.setTimeOnPage(Duration.ofSeconds(5 + random.nextInt(300)));

        // Schema 演化：scrollDepth 可选字段
        if (random.nextDouble() < schemaEvolutionProbability) {
            event.setScrollDepth(random.nextInt(100));
        }

        event.addMetadata("source", "data-generator");
        event.addMetadata("schema_version", event.getScrollDepth() != null ? "v2" : "v1");
        return event;
    }

    // ==================== 商品价格变更事件 ====================

    public ProductPriceEvent createProductPriceEvent() {
        String eventId = generateEventId("PRODUCT_PRICE");
        Instant eventTime = generateEventTime();
        String region = selectRegion();
        LocalDate eventDate = eventTime.atZone(ZoneOffset.UTC).toLocalDate();

        ProductInfo product = randomProduct();
        // 价格在基准价的 50%~150% 区间波动
        BigDecimal newPrice = product.basePrice()
                .multiply(BigDecimal.valueOf(0.5 + random.nextDouble()))
                .setScale(2, RoundingMode.HALF_UP);

        ProductPriceEvent event = new ProductPriceEvent(eventId, eventTime, region, eventDate,
                product.productId(), newPrice);
        event.addMetadata("source", "data-generator");
        event.addMetadata("schema_version", "v1");
        return event;
    }

    // ==================== 辅助方法 ====================

    private String selectEventType() {
        double rand = random.nextDouble();
        double cumulative = 0.0;
        for (Map.Entry<String, Double> entry : eventTypeDistribution.entrySet()) {
            cumulative += entry.getValue();
            if (rand <= cumulative) {
                return entry.getKey();
            }
        }
        return eventTypeDistribution.keySet().iterator().next();
    }

    private String generateEventId(String eventType) {
        Set<String> typePool = reusableEventIdsByType
                .computeIfAbsent(eventType, k -> new HashSet<>());

        if (!typePool.isEmpty() && random.nextDouble() < duplicateKeyProbability) {
            List<String> ids = new java.util.ArrayList<>(typePool);
            return ids.get(random.nextInt(ids.size()));
        }

        String newId = eventType + "-" + UUID.randomUUID();
        if (typePool.size() < maxReusableIdsPerType) {
            typePool.add(newId);
        }
        return newId;
    }

    private Instant generateEventTime() {
        Instant now = Instant.now();
        if (random.nextDouble() < lateArrivalProbability) {
            long lateMinutes = 1 + random.nextInt(60);
            return now.minus(Duration.ofMinutes(lateMinutes));
        }
        return now.minusMillis(random.nextInt(5000));
    }

    private String selectRegion() {
        if (hotRegionProbability <= 0.0) {
            return randomElement(REGIONS);
        }
        if (random.nextDouble() < hotRegionProbability) {
            return HOT_REGION;
        }
        return REGIONS[1 + random.nextInt(REGIONS.length - 1)];
    }

    private <T> T randomElement(T[] array) {
        return array[random.nextInt(array.length)];
    }

    private ProductInfo randomProduct() {
        return PRODUCT_CATALOG.get(random.nextInt(PRODUCT_CATALOG.size()));
    }

    private double clamp(double value) {
        return Math.max(0.0, Math.min(1.0, value));
    }

    // ==================== Getter ====================

    public Map<String, Double> getEventTypeDistribution() {
        return new HashMap<>(eventTypeDistribution);
    }

    public double getDuplicateKeyProbability() {
        return duplicateKeyProbability;
    }

    public double getLateArrivalProbability() {
        return lateArrivalProbability;
    }

    public double getSchemaEvolutionProbability() {
        return schemaEvolutionProbability;
    }

    public double getHotRegionProbability() {
        return hotRegionProbability;
    }

    public String getHotRegion() {
        return HOT_REGION;
    }
}
