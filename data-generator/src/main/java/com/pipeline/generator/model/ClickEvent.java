package com.pipeline.generator.model;

import lombok.Getter;
import lombok.Setter;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;

/**
 * 页面点击事件。
 *
 * <p>模拟用户浏览页面的行为：会话、页面 URL、来源、停留时长。
 * scrollDepth 是可选字段（Schema 演化场景），v1 不出现，v2 出现。</p>
 *
 * <p>customerId 字段用于与订单事件（orders topic）按用户维度做 Interval Join：
 * 关联用户下单前一段时间内的点击行为，补全 pageUrl / referrer 到订单宽表。</p>
 */
@Getter
public class ClickEvent extends BaseEvent {

    /** 会话 ID，格式 SESSION-xxxxxxxxxxxx。 */
    private final String sessionId;

    /** 客户号，格式 CUST-数字，与订单事件的 customerId 取值范围一致（0~9999），用于双流关联。 */
    private final String customerId;

    /** 商品号，格式 PROD-xxxx，引用 EventFactory 内置商品目录。表示用户点击浏览的商品页面。 */
    private final String productId;

    /** 页面 URL，格式 https://example.com/pageN。 */
    private final String pageUrl;

    /** 来源页面：google 或本站。 */
    private final String referrer;

    /** 页面停留时长（秒）。 */
    @Setter
    private Duration timeOnPage;

    /** 滚动深度百分比（0~100）。可选字段，用于模拟 Schema 演化（v2 才有）。 */
    @Setter
    private Integer scrollDepth;

    public ClickEvent(String eventId, Instant eventTime, String region, LocalDate eventDate,
                      String sessionId, String customerId, String productId, String pageUrl, String referrer) {
        super(eventId, eventTime, region, eventDate, "CLICK");
        this.sessionId = sessionId;
        this.customerId = customerId;
        this.productId = productId;
        this.pageUrl = pageUrl;
        this.referrer = referrer;
    }
}
