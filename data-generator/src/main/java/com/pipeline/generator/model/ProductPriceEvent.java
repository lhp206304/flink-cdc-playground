package com.pipeline.generator.model;

import lombok.Getter;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

/**
 * 商品价格变更事件。
 *
 * <p>模拟商品实时改价：只包含商品号和新价格，不包含完整商品信息。
 * 完整商品信息（名称、类目等）走 PostgreSQL CDC，Kafka 只发价格变更事件。</p>
 *
 * <p>用途：下游 Flink 作业消费后，可更新缓存中的商品价格，
 * 或与订单流关联计算实时金额。</p>
 */
@Getter
public class ProductPriceEvent extends BaseEvent {

    /** 商品号，格式 PROD-xxxx。 */
    private final String productId;

    /** 变更后的新价格，保留两位小数。 */
    private final BigDecimal newPrice;

    public ProductPriceEvent(String eventId, Instant eventTime, String region,
                             LocalDate eventDate, String productId, BigDecimal newPrice) {
        super(eventId, eventTime, region, eventDate, "PRODUCT_PRICE");
        this.productId = productId;
        this.newPrice = newPrice;
    }
}
