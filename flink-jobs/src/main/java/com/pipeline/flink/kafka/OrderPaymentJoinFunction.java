package com.pipeline.flink.kafka;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

import com.pipeline.flink.kafka.model.CdcEnvelope;
import com.pipeline.flink.kafka.model.Payment;
import com.pipeline.flink.kafka.model.dwd.EnrichedOrder;
import com.pipeline.flink.kafka.model.dwd.OrderPaymentWide;

/**
 * 订单 + 支付双流关联。
 * <p>
 * 使用 KeyedCoProcessFunction，按 order_id keyBy 后关联两个流：
 * <ul>
 *   <li>流1：EnrichedOrder（已富化地区和用户维度的订单）</li>
 *   <li>流2：CdcEnvelope<Payment>（支付 CDC 数据）</li>
 * </ul>
 * <p>
 * 关联逻辑：
 * <ul>
 *   <li>订单到达：缓存订单状态；若支付已到达，直接输出宽表</li>
 *   <li>支付到达：缓存支付状态；若订单已到达，直接输出宽表</li>
 *   <li>两边都到达后，输出 OrderPaymentWide</li>
 * </ul>
 */
public class OrderPaymentJoinFunction
        extends KeyedCoProcessFunction<Long, EnrichedOrder, CdcEnvelope<Payment>, OrderPaymentWide> {

    private static final long serialVersionUID = 1L;

    /** 订单状态缓存。 */
    private transient ValueState<EnrichedOrder> orderState;

    /** 支付状态缓存。 */
    private transient ValueState<Payment> paymentState;

    @Override
    public void open(OpenContext openContext) {
        // 状态 TTL：1 小时未更新则自动清理，防止孤儿数据导致状态无限膨胀
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofHours(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();

        ValueStateDescriptor<EnrichedOrder> orderDesc =
                new ValueStateDescriptor<>("order_state", EnrichedOrder.class);
        orderDesc.enableTimeToLive(ttlConfig);
        orderState = getRuntimeContext().getState(orderDesc);

        ValueStateDescriptor<Payment> paymentDesc =
                new ValueStateDescriptor<>("payment_state", Payment.class);
        paymentDesc.enableTimeToLive(ttlConfig);
        paymentState = getRuntimeContext().getState(paymentDesc);
    }

    /**
     * 处理订单流（流1）。
     */
    @Override
    public void processElement1(
            EnrichedOrder order,
            Context ctx,
            Collector<OrderPaymentWide> out) throws Exception {
        // 缓存最新订单
        orderState.update(order);

        // 检查支付是否已到达
        Payment payment = paymentState.value();
        if (payment != null) {
            out.collect(buildWideTable(order, payment));
        }
    }

    /**
     * 处理支付流（流2）。
     */
    @Override
    public void processElement2(
            CdcEnvelope<Payment> envelope,
            Context ctx,
            Collector<OrderPaymentWide> out) throws Exception {
        // 跳过 DELETE 操作（支付被删除时不输出宽表）
        if (envelope.isDelete() || envelope.getAfter() == null) {
            return;
        }

        Payment payment = envelope.getAfter();
        // 缓存最新支付
        paymentState.update(payment);

        // 检查订单是否已到达
        EnrichedOrder order = orderState.value();
        if (order != null) {
            out.collect(buildWideTable(order, payment));
        }
    }

    /**
     * 合并订单和支付，构建 DWD 宽表。
     */
    private OrderPaymentWide buildWideTable(EnrichedOrder order, Payment payment) {
        OrderPaymentWide wide = new OrderPaymentWide();
        // 订单字段
        wide.setOrder_id(order.getOrder_id());
        wide.setUser_id(order.getUser_id());
        wide.setRegion_code(order.getRegion_code());
        wide.setTotal_amount(order.getTotal_amount());
        wide.setDiscount_amount(order.getDiscount_amount());
        wide.setOrder_status(order.getStatus());
        wide.setOrder_created_at(order.getCreated_at());
        // 支付字段
        wide.setPayment_id(payment.getPayment_id());
        wide.setPayment_amount(payment.getAmount());
        wide.setPay_method(payment.getPay_method());
        wide.setPayment_status(payment.getStatus());
        wide.setPayment_created_at(payment.getCreated_at());
        // 维度字段
        wide.setRegion_name(order.getRegion_name());
        wide.setUser_name(order.getUser_name());
        wide.setUser_level(order.getUser_level());
        return wide;
    }
}
