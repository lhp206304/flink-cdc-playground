package com.pipeline.generator.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.math.BigDecimal;

/**
 * 订单明细项。
 *
 * <p>一个订单可包含多个商品，每个商品对应一个 OrderItem。
 * productName 和 productPrice 是下单时的快照，防止商品后续改名/改价影响历史订单。</p>
 */
@Getter
@AllArgsConstructor
public class OrderItem {

    /** 商品号，格式 PROD-xxxx。 */
    private final String productId;

    /** 下单时的商品名称（快照）。 */
    private final String productName;

    /** 下单时的商品单价（快照）。 */
    private final BigDecimal productPrice;

    /** 购买数量。 */
    private final int quantity;
}
