package com.pipeline.flink.kafka.model.event;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.pipeline.flink.common.annotation.EventEntity;

/**
 * 商品价格变更事件。
 * <p>
 * 对应 Kafka 的 events.product_price topic，由数据生成器直接写入。
 * 只包含商品号和新价格，完整商品信息走维表（base.products）。
 */
@EventEntity
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductPriceEvent extends BaseEvent {

    /** 商品号。 */
    private String productId;

    /** 变更后的新价格。 */
    private BigDecimal newPrice;

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public BigDecimal getNewPrice() {
        return newPrice;
    }

    public void setNewPrice(BigDecimal newPrice) {
        this.newPrice = newPrice;
    }
}
