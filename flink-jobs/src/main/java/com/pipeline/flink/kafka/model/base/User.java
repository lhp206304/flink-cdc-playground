package com.pipeline.flink.kafka.model.base;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 用户实体。
 * <p>
 * 对应 PostgreSQL base.users 表，中表（万级行），
 * 通过 AsyncIO + JDBC 实时查询，按 user_id 查找。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class User {

    /** 用户 ID（主键）。 */
    private Long user_id;

    /** 用户名。 */
    private String user_name;

    /** 邮箱。 */
    private String email;

    /** 手机号。 */
    private String phone;

    /** 城市编码。 */
    private String city_code;

    /** 性别：0=女，1=男。 */
    private Short gender;

    /** 生日。 */
    private LocalDate birthday;

    /** 会员等级。 */
    private Integer member_level;

    /** 用户等级：gold/silver/bronze。 */
    private String user_level;

    /** 年龄。 */
    private Integer age;

    /** 注册日期。 */
    private LocalDate register_date;

    /** 累计订单数。 */
    private Integer total_orders;

    /** 累计消费金额。 */
    private BigDecimal total_amount;

    /** 状态：1=正常，0=禁用。 */
    private Short status;

    /** 创建时间。 */
    private LocalDateTime created_at;

    /** 更新时间。 */
    private LocalDateTime updated_at;

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getCity_code() {
        return city_code;
    }

    public void setCity_code(String city_code) {
        this.city_code = city_code;
    }

    public Short getGender() {
        return gender;
    }

    public void setGender(Short gender) {
        this.gender = gender;
    }

    public LocalDate getBirthday() {
        return birthday;
    }

    public void setBirthday(LocalDate birthday) {
        this.birthday = birthday;
    }

    public Integer getMember_level() {
        return member_level;
    }

    public void setMember_level(Integer member_level) {
        this.member_level = member_level;
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

    public LocalDate getRegister_date() {
        return register_date;
    }

    public void setRegister_date(LocalDate register_date) {
        this.register_date = register_date;
    }

    public Integer getTotal_orders() {
        return total_orders;
    }

    public void setTotal_orders(Integer total_orders) {
        this.total_orders = total_orders;
    }

    public BigDecimal getTotal_amount() {
        return total_amount;
    }

    public void setTotal_amount(BigDecimal total_amount) {
        this.total_amount = total_amount;
    }

    public Short getStatus() {
        return status;
    }

    public void setStatus(Short status) {
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
