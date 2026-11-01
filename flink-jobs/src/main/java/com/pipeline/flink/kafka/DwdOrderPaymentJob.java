package com.pipeline.flink.kafka;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pipeline.flink.common.IcebergSinkFactory;
import com.pipeline.flink.common.KafkaSourceFactory;
import com.pipeline.flink.common.StreamEnvBase;
import com.pipeline.flink.common.serde.DeserializationResult;
import com.pipeline.flink.kafka.enrichment.RegionBroadcastJoin;
import com.pipeline.flink.kafka.enrichment.UserAsyncIoJoin;
import com.pipeline.flink.kafka.model.CdcEnvelope;
import com.pipeline.flink.kafka.model.Order;
import com.pipeline.flink.kafka.model.Payment;
import com.pipeline.flink.kafka.model.base.Region;
import com.pipeline.flink.kafka.model.dwd.EnrichedOrder;
import com.pipeline.flink.kafka.model.dwd.OrderPaymentWide;

/**
 * DWD 层作业：订单支付宽表。
 * <p>
 * 消费 CDC 订单流（cdc.orders）和支付流（cdc.payments），
 * 经过地区维表（Broadcast）+ 用户维表（AsyncIO）富化后，
 * 按 order_id 双流关联，产出订单支付宽表写入 Iceberg。
 * <p>
 * 处理流程：
 * 1. 消费 Kafka：cdc.orders、cdc.payments、cdc.base_region
 * 2. 反序列化 + 脏数据侧输出到 DLQ
 * 3. 订单流取 after → EnrichedOrder
 * 4. Broadcast 关联 base.region（CDC 变更流驱动）
 * 5. AsyncIO 关联 base.users（用户画像）
 * 6. 按 order_id 与支付流 KeyedCoProcessFunction 关联
 * 7. 写入 Iceberg：dwd.order_payment_wide（append）
 */
public class DwdOrderPaymentJob {

    private static final Logger LOG = LoggerFactory.getLogger(DwdOrderPaymentJob.class);

    /** DLQ 侧输出标签。 */
    private static final OutputTag<String> DLQ_CDC_TAG = new OutputTag<String>("dlq-cdc") {};

    public static void main(String[] args) throws Exception {
        LOG.info("===== DwdOrderPaymentJob 启动 =====");

        // 1. 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamEnvBase.createEnv();

        // 2. 定义 Kafka Source
        KafkaSource<DeserializationResult<CdcEnvelope<Order>>> ordersSource =
                KafkaSourceFactory.buildCdcSource("cdc.orders", Order.class);
        KafkaSource<DeserializationResult<CdcEnvelope<Payment>>> paymentsSource =
                KafkaSourceFactory.buildCdcSource("cdc.payments", Payment.class);

        // 3. 注册 Source → DataStream
        DataStream<DeserializationResult<CdcEnvelope<Order>>> ordersRaw =
                env.fromSource(ordersSource, WatermarkStrategy.noWatermarks(), "cdc.orders-source");
        DataStream<DeserializationResult<CdcEnvelope<Payment>>> paymentsRaw =
                env.fromSource(paymentsSource, WatermarkStrategy.noWatermarks(), "cdc.payments-source");

        // 4. 分流：成功 → 主流，失败 → DLQ
        SingleOutputStreamOperator<CdcEnvelope<Order>> ordersEnvelopeStream =
                ordersRaw.process(new CdcDeserializationSplitter<>(DLQ_CDC_TAG))
                        .name("orders-dlq-splitter");
        SingleOutputStreamOperator<CdcEnvelope<Payment>> paymentsEnvelopeStream =
                paymentsRaw.process(new CdcDeserializationSplitter<>(DLQ_CDC_TAG))
                        .name("payments-dlq-splitter");

        // 5. 订单流：取 after 字段 → EnrichedOrder
        DataStream<EnrichedOrder> enrichedOrderStream = ordersEnvelopeStream
                .process(new ProcessFunction<CdcEnvelope<Order>, EnrichedOrder>() {
                    @Override
                    public void processElement(
                            CdcEnvelope<Order> envelope,
                            Context ctx,
                            Collector<EnrichedOrder> out) {
                        // 跳过 DELETE（after 为 null）
                        if (envelope.getAfter() != null) {
                            out.collect(new EnrichedOrder(envelope.getAfter()));
                        }
                    }
                })
                .name("orders-extract-after");

        // 6. 地区维表 CDC 流（cdc.base_region）+ Broadcast 关联
        KafkaSource<DeserializationResult<CdcEnvelope<Region>>> regionSource =
                KafkaSourceFactory.buildCdcSource("cdc.base_region", Region.class);
        SingleOutputStreamOperator<CdcEnvelope<Region>> regionEnvelopeStream =
                env.fromSource(regionSource, WatermarkStrategy.noWatermarks(), "region-source")
                        .process(new CdcDeserializationSplitter<>(DLQ_CDC_TAG))
                        .name("region-dlq-splitter");
        DataStream<EnrichedOrder> orderWithRegionStream = enrichedOrderStream
                .connect(regionEnvelopeStream.broadcast(RegionBroadcastJoin.REGION_STATE_DESC))
                .process(new RegionBroadcastJoin())
                .name("region-broadcast-join");

        // 7. 用户维表 AsyncIO 关联
        DataStream<EnrichedOrder> enrichedOrderWithDimStream = AsyncDataStream.unorderedWait(
                orderWithRegionStream,
                new UserAsyncIoJoin(),
                5000L,      // 超时时间（毫秒）
                TimeUnit.MILLISECONDS,
                100)         // 最大并发请求数
                .name("user-asyncio-join");

        // 8. 双流 JOIN：订单（富化后）+ 支付，按 order_id 关联
        DataStream<OrderPaymentWide> wideTableStream = enrichedOrderWithDimStream
                .keyBy(EnrichedOrder::getOrder_id)
                .connect(paymentsEnvelopeStream.keyBy(env2 -> env2.getAfter() != null ? env2.getAfter().getOrder_id() : -1L))
                .process(new OrderPaymentJoinFunction())
                .name("order-payment-join");

        // 9. 写入 Iceberg（Table API 方式：自动建表 + INSERT INTO）
        // writeToIceberg 内部调用 result.await() 阻塞，流式作业持续运行
        StreamTableEnvironment tableEnv = IcebergSinkFactory.createTableEnv(env);
        Schema wideSchema = buildWideTableSchema();
        IcebergSinkFactory.writeToIceberg(tableEnv, wideTableStream, wideSchema, "order_payment_wide");

        // 10. DLQ：三个流的反序列化失败消息统一写入 dlq.cdc topic
        KafkaSink<String> dlqSink = KafkaSourceFactory.buildDlqSink();
        ordersEnvelopeStream.getSideOutput(DLQ_CDC_TAG).sinkTo(dlqSink).name("orders-dlq-sink");
        paymentsEnvelopeStream.getSideOutput(DLQ_CDC_TAG).sinkTo(dlqSink).name("payments-dlq-sink");
        regionEnvelopeStream.getSideOutput(DLQ_CDC_TAG).sinkTo(dlqSink).name("region-dlq-sink");

        // 注意：不再调用 env.execute()，INSERT 作业已由 writeToIceberg 提交并阻塞
    }

    /**
     * 构建订单支付宽表的 Flink Table Schema。
     * <p>
     * 字段名/类型必须与 {@link OrderPaymentWide} 实体一致，
     * Table API 据此完成 DataStream → Table 转换及 Iceberg 建表。
     */
    private static Schema buildWideTableSchema() {
        return Schema.newBuilder()
                .column("order_id", DataTypes.BIGINT())
                .column("user_id", DataTypes.BIGINT())
                .column("region_code", DataTypes.STRING())
                .column("total_amount", DataTypes.DECIMAL(12, 2))
                .column("discount_amount", DataTypes.DECIMAL(10, 2))
                .column("order_status", DataTypes.STRING())
                .column("order_created_at", DataTypes.TIMESTAMP(6))
                .column("payment_id", DataTypes.BIGINT())
                .column("payment_amount", DataTypes.DECIMAL(12, 2))
                .column("pay_method", DataTypes.STRING())
                .column("payment_status", DataTypes.STRING())
                .column("payment_created_at", DataTypes.TIMESTAMP(6))
                .column("region_name", DataTypes.STRING())
                .column("user_name", DataTypes.STRING())
                .column("user_level", DataTypes.STRING())
                .build();
    }
}
