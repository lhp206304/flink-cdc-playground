package com.pipeline.flink.kafka.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.pipeline.flink.common.annotation.CdcEntity;

/**
 * 订单实体。
 * <p>
 * 对应 PostgreSQL public.orders 表，通过 CDC 同步到 Kafka 的 cdc.orders topic。
 * 字段名与数据库列名保持一致，Jackson 反序列化时自动映射。
 */
@CdcEntity
@JsonIgnoreProperties(ignoreUnknown = true)
public class Order {

    /** 订单 ID（自增主键）。 */
    private Long order_id;

    /** 用户 ID，关联 base.users.user_id。 */
    private Long user_id;

    /** 地区编码，关联 base.region.region_code。 */
    private String region_code;

    /** 订单总金额。 */
    private BigDecimal total_amount;

    /** 优惠金额。 */
    private BigDecimal discount_amount;

    /** 优惠券 ID。 */
    private Long coupon_id;

    /** 支付方式。 */
    private String payment_method;

    /** 收货地址。 */
    private String shipping_address;

    /** 订单状态。 */
    private String status;

    /** 备注。 */
    private String remark;

    /** 创建时间。 */
    private LocalDateTime created_at;

    /** 更新时间。 */
    private LocalDateTime updated_at;

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

    public Long getCoupon_id() {
        return coupon_id;
    }

    public void setCoupon_id(Long coupon_id) {
        this.coupon_id = coupon_id;
    }

    public String getPayment_method() {
        return payment_method;
    }

    public void setPayment_method(String payment_method) {
        this.payment_method = payment_method;
    }

    public String getShipping_address() {
        return shipping_address;
    }

    public void setShipping_address(String shipping_address) {
        this.shipping_address = shipping_address;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
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
