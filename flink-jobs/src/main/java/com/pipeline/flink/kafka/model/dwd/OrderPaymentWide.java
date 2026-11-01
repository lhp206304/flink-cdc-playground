package com.pipeline.flink.kafka.model.dwd;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * DWD 层订单支付宽表。
 * <p>
 * 由订单富化流（EnrichedOrder）与支付流（Payment）按 order_id 关联后产出，
 * 写入 Iceberg 的 dwd.order_payment_wide 表（append 模式）。
 */
public class OrderPaymentWide {

    // ===== 订单字段 =====
    private Long order_id;
    private Long user_id;
    private String region_code;
    private BigDecimal total_amount;
    private BigDecimal discount_amount;
    private String order_status;
    private LocalDateTime order_created_at;

    // ===== 支付字段 =====
    private Long payment_id;
    private BigDecimal payment_amount;
    private String pay_method;
    private String payment_status;
    private LocalDateTime payment_created_at;

    // ===== 维度字段 =====
    private String region_name;
    private String user_name;
    private String user_level;

    public Long getOrder_id() {
        return order_id;
    }

    public void setOrder_id(Long order_id) {
        this.order_id = order_id;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public String getRegion_code() {
        return region_code;
    }

    public void setRegion_code(String region_code) {
        this.region_code = region_code;
    }

    public BigDecimal getTotal_amount() {
        return total_amount;
    }

    public void setTotal_amount(BigDecimal total_amount) {
        this.total_amount = total_amount;
    }

    public BigDecimal getDiscount_amount() {
        return discount_amount;
    }

    public void setDiscount_amount(BigDecimal discount_amount) {
        this.discount_amount = discount_amount;
    }

    public String getOrder_status() {
        return order_status;
    }

    public void setOrder_status(String order_status) {
        this.order_status = order_status;
    }

    public LocalDateTime getOrder_created_at() {
        return order_created_at;
    }

    public void setOrder_created_at(LocalDateTime order_created_at) {
        this.order_created_at = order_created_at;
    }

    public Long getPayment_id() {
        return payment_id;
    }

    public void setPayment_id(Long payment_id) {
        this.payment_id = payment_id;
    }

    public BigDecimal getPayment_amount() {
        return payment_amount;
    }

    public void setPayment_amount(BigDecimal payment_amount) {
        this.payment_amount = payment_amount;
    }

    public String getPay_method() {
        return pay_method;
    }

    public void setPay_method(String pay_method) {
        this.pay_method = pay_method;
    }

    public String getPayment_status() {
        return payment_status;
    }

    public void setPayment_status(String payment_status) {
        this.payment_status = payment_status;
    }

    public LocalDateTime getPayment_created_at() {
        return payment_created_at;
    }

    public void setPayment_created_at(LocalDateTime payment_created_at) {
        this.payment_created_at = payment_created_at;
    }

    public String getRegion_name() {
        return region_name;
    }

    public void setRegion_name(String region_name) {
        this.region_name = region_name;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public String getUser_level() {
        return user_level;
    }

    public void setUser_level(String user_level) {
        this.user_level = user_level;
    }
}
