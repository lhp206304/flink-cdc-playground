package com.pipeline.flink.kafka.model.event;

import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.pipeline.flink.common.annotation.EventEntity;

/**
 * 页面点击事件。
 * <p>
 * 对应 Kafka 的 events.clicks topic，由数据生成器直接写入。
 * 用于与商品维表、用户维表关联，富化用户浏览行为。
 */
@EventEntity
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClickEvent extends BaseEvent {

    /** 会话 ID。 */
    private String sessionId;

    /** 客户号，用于与订单按用户维度关联。 */
    private String customerId;

    /** 商品号，用户点击浏览的商品页面。 */
    private String productId;

    /** 页面 URL。 */
    private String pageUrl;

    /** 来源页面。 */
    private String referrer;

    /** 页面停留时长（秒）。 */
    private Duration timeOnPage;

    /** 滚动深度百分比（0~100）。可选字段，Schema 演化场景用。 */
    private Integer scrollDepth;

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getPageUrl() {
        return pageUrl;
    }

    public void setPageUrl(String pageUrl) {
        this.pageUrl = pageUrl;
    }

    public String getReferrer() {
        return referrer;
    }

    public void setReferrer(String referrer) {
        this.referrer = referrer;
    }

    public Duration getTimeOnPage() {
        return timeOnPage;
    }

    public void setTimeOnPage(Duration timeOnPage) {
        this.timeOnPage = timeOnPage;
    }

    public Integer getScrollDepth() {
        return scrollDepth;
    }

    public void setScrollDepth(Integer scrollDepth) {
        this.scrollDepth = scrollDepth;
    }
}
