package com.pipeline.generator.model;

import lombok.Getter;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/**
 * 订单事件。
 *
 * <p>模拟电商下单行为，支持一单多商品（items 列表）。
 * 包含优惠金额、支付方式、收货地址等真实电商字段。
 * status 字段用于练习"状态变更"和"超时检测"。</p>
 */
@Getter
public class OrderEvent extends BaseEvent {

    /** 订单号，格式 ORD-xxxxxxxx。 */
    private final String orderId;

    /** 客户号，格式 CUST-数字。 */
    private final String customerId;

    /** 订单明细列表（一单多商品）。 */
    private final List<OrderItem> items;

    /** 订单总金额（items 金额之和），保留两位小数。 */
    private final BigDecimal totalAmount;

    /** 优惠金额，保留两位小数。 */
    private final BigDecimal discountAmount;

    /** 支付方式：alipay/wechat/card。 */
    private final String paymentMethod;

    /** 收货地址。 */
    private final String shippingAddress;

    /** 币种：USD/EUR/GBP/JPY。 */
    private final String currency;

    /** 订单状态：pending/confirmed/shipped/delivered/cancelled。 */
    private final String status;

    public OrderEvent(String eventId, Instant eventTime, String region, LocalDate eventDate,
                      String orderId, String customerId, List<OrderItem> items,
                      BigDecimal totalAmount, BigDecimal discountAmount,
                      String paymentMethod, String shippingAddress, String currency, String status) {
        super(eventId, eventTime, region, eventDate, "ORDER");
        this.orderId = orderId;
        this.customerId = customerId;
        this.items = items;
        this.totalAmount = totalAmount;
        this.discountAmount = discountAmount;
        this.paymentMethod = paymentMethod;
        this.shippingAddress = shippingAddress;
        this.currency = currency;
        this.status = status;
    }
}
