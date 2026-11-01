package com.pipeline.flink.kafka.model.dwd;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.pipeline.flink.kafka.model.Order;

/**
 * 订单富化实体（订单 + 地区维度 + 用户维度）。
 * <p>
 * 由 orders CDC 流经过 Broadcast（base.region）和 AsyncIO（base.users）关联后产出。
 */
public class EnrichedOrder {

    // ===== 订单字段 =====
    private Long order_id;
    private Long user_id;
    private String region_code;
    private BigDecimal total_amount;
    private BigDecimal discount_amount;
    private Long coupon_id;
    private String payment_method;
    private String shipping_address;
    private String status;
    private String remark;
    private LocalDateTime created_at;
    private LocalDateTime updated_at;

    // ===== 地区维度字段（base.region）=====
    private String region_name;
    private String province;
    private String city;
    private String country;

    // ===== 用户维度字段（base.users）=====
    private String user_name;
    private String user_level;
    private Integer age;
    private String city_code;

    /**
     * 无参构造函数（Flink 序列化需要）。
     */
    public EnrichedOrder() {
    }

    /**
     * 从 Order 实体构造 EnrichedOrder。
     * <p>
     * 仅赋值订单字段，地区/用户维度字段保持 null，
     * 由后续 Broadcast（base.region）和 AsyncIO（base.users）关联补充。
     *
     * @param order CDC 订单实体
     */
    public EnrichedOrder(Order order) {
        this.order_id = order.getOrder_id();
        this.user_id = order.getUser_id();
        this.region_code = order.getRegion_code();
        this.total_amount = order.getTotal_amount();
        this.discount_amount = order.getDiscount_amount();
        this.coupon_id = order.getCoupon_id();
        this.payment_method = order.getPayment_method();
        this.shipping_address = order.getShipping_address();
        this.status = order.getStatus();
        this.remark = order.getRemark();
        this.created_at = order.getCreated_at();
        this.updated_at = order.getUpdated_at();
    }

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

    public String getRegion_name() {
        return region_name;
    }

    public void setRegion_name(String region_name) {
        this.region_name = region_name;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
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

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getCity_code() {
        return city_code;
    }

    public void setCity_code(String city_code) {
        this.city_code = city_code;
    }
}
