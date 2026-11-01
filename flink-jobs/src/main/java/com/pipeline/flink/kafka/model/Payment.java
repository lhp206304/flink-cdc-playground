package com.pipeline.flink.kafka.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.pipeline.flink.common.annotation.CdcEntity;

/**
 * 支付实体。
 * <p>
 * 对应 PostgreSQL public.payments 表，通过 CDC 同步到 Kafka 的 cdc.payments topic。
 * 与 Order 通过 order_id 关联。
 */
@CdcEntity
@JsonIgnoreProperties(ignoreUnknown = true)
public class Payment {

    /** 支付 ID（自增主键）。 */
    private Long payment_id;

    /** 订单 ID，关联 public.orders.order_id。 */
    private Long order_id;

    /** 支付金额。 */
    private BigDecimal amount;

    /** 支付方式。 */
    private String pay_method;

    /** 支付状态。 */
    private String status;

    /** 创建时间。 */
    private LocalDateTime created_at;

    /** 更新时间。 */
    private LocalDateTime updated_at;

    public Long getPayment_id() {
        return payment_id;
    }

    public void setPayment_id(Long payment_id) {
        this.payment_id = payment_id;
    }

    public Long getOrder_id() {
        return order_id;
    }

    public void setOrder_id(Long order_id) {
        this.order_id = order_id;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getPay_method() {
        return pay_method;
    }

    public void setPay_method(String pay_method) {
        this.pay_method = pay_method;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getCreated_at() {
        return created_at;
    }

    public void setCreated_at(LocalDateTime created_at) {
        this.created_at = created_at;
    }

    public LocalDateTime getUpdated_at() {
        return updated_at;
    }

    public void setUpdated_at(LocalDateTime updated_at) {
        this.updated_at = updated_at;
    }
}
