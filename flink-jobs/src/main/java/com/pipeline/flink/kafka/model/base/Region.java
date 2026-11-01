package com.pipeline.flink.kafka.model.base;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 地区实体。
 * <p>
 * 对应 PostgreSQL base.region 表，小表（< 1000 行），
 * 通过 Broadcast 广播流全量加载到内存，按 region_code 查找。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Region {

    /** 地区编码（主键）。 */
    private String region_code;

    /** 地区名称。 */
    private String region_name;

    /** 省份。 */
    private String province;

    /** 城市。 */
    private String city;

    /** 国家。 */
    private String country;

    /** 时区。 */
    private String timezone;

    /** 更新时间。 */
    private LocalDateTime updated_at;

    public String getRegion_code() {
        return region_code;
    }

    public void setRegion_code(String region_code) {
        this.region_code = region_code;
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

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public LocalDateTime getUpdated_at() {
        return updated_at;
    }

    public void setUpdated_at(LocalDateTime updated_at) {
        this.updated_at = updated_at;
    }
}
