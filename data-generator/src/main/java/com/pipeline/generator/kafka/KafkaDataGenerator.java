package com.pipeline.generator.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pipeline.generator.model.BaseEvent;
import com.pipeline.generator.model.ProductPriceEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka 事件生成器主类：按固定速率生成事件并发送到 Kafka。
 *
 * <p>事件类型与 Topic 映射：</p>
 * <ul>
 *   <li>CLICK → events.clicks（用户点击事件）</li>
 *   <li>PRODUCT_PRICE → events.product_price（商品价格变更事件）</li>
 * </ul>
 *
 * <p>注意：订单数据走 PostgreSQL CDC（cdc.orders），不在此生成事件流，避免数据重复。</p>
 *
 * <p>环境变量配置（缺失即报错退出，遵循"单一数据源、无默认值"约定）：</p>
 * <ul>
 *   <li>KAFKA_BOOTSTRAP_SERVERS：Kafka 地址</li>
 *   <li>GENERATOR_TOPICS：topic 速率配置，格式 orders:30,clicks:50,product_price:20</li>
 *   <li>GENERATOR_DURATION_SECONDS：运行时长（秒），-1 表示持续运行</li>
 *   <li>DUPLICATE_KEY_PROBABILITY：重复主键概率</li>
 *   <li>LATE_ARRIVAL_PROBABILITY：迟到事件概率</li>
 *   <li>SCHEMA_EVOLUTION_PROBABILITY：Schema 演化概率</li>
 *   <li>HOT_REGION_PROBABILITY：热点 region 概率</li>
 * </ul>
 */
public class KafkaDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaDataGenerator.class);

    private static final int BATCH_SIZE = 500;

    private final String bootstrapServers;
    private final int totalEventsPerSecond;
    private final int durationSeconds;
    private final Producer<String, String> producer;
    private final EventFactory eventFactory;
    private final ObjectMapper objectMapper;
    private final AtomicLong totalEventsSent = new AtomicLong(0);
    private final AtomicLong totalBytesSent = new AtomicLong(0);

    public KafkaDataGenerator() {
        this.bootstrapServers = getRequiredEnv("KAFKA_BOOTSTRAP_SERVERS");
        this.totalEventsPerSecond = parseTotalRate(getRequiredEnv("GENERATOR_TOPICS"));
        this.durationSeconds = Integer.parseInt(getRequiredEnv("GENERATOR_DURATION_SECONDS"));

        double duplicateProb = Double.parseDouble(getRequiredEnv("DUPLICATE_KEY_PROBABILITY"));
        double lateProb = Double.parseDouble(getRequiredEnv("LATE_ARRIVAL_PROBABILITY"));
        double schemaProb = Double.parseDouble(getRequiredEnv("SCHEMA_EVOLUTION_PROBABILITY"));
        double hotRegionProb = Double.parseDouble(getRequiredEnv("HOT_REGION_PROBABILITY"));

        this.producer = createKafkaProducer();
        this.eventFactory = new EventFactory(new HashMap<>(), duplicateProb, lateProb, schemaProb, hotRegionProb);

        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public void run() {
        LOG.info("===== Kafka 事件生成器启动 =====");
        LOG.info("Kafka: {}", bootstrapServers);
        LOG.info("速率: {} events/秒", totalEventsPerSecond);
        LOG.info("模式: {}", durationSeconds < 0 ? "持续运行" : durationSeconds + " 秒");
        LOG.info("事件分布: {}", eventFactory.getEventTypeDistribution());

        long startTime = System.currentTimeMillis();
        long lastLogTime = startTime;
        long lastEventCount = 0;

        try {
            while (shouldContinue(startTime)) {
                long batchStartTime = System.currentTimeMillis();
                sendBatch();

                long currentTime = System.currentTimeMillis();
                if (currentTime - lastLogTime >= 5000) {
                    long currentCount = totalEventsSent.get();
                    long eventsSinceLastLog = currentCount - lastEventCount;
                    double elapsedSec = (currentTime - lastLogTime) / 1000.0;
                    double throughput = elapsedSec > 0 ? eventsSinceLastLog / elapsedSec : 0;

                    LOG.info("进度: {} 条已发送, {} MB, 当前吞吐: {} 条/秒",
                            currentCount,
                            String.format("%.3f", totalBytesSent.get() / (1024.0 * 1024.0)),
                            String.format("%.1f", throughput));

                    lastLogTime = currentTime;
                    lastEventCount = currentCount;
                }

                long batchDuration = System.currentTimeMillis() - batchStartTime;
                long targetDuration = 1000;
                if (batchDuration < targetDuration) {
                    Thread.sleep(targetDuration - batchDuration);
                }
            }

            LOG.info("===== Kafka 事件生成器结束 =====");
            LOG.info("总事件数: {}", totalEventsSent.get());

        } catch (InterruptedException e) {
            LOG.warn("生成器被中断", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.error("生成器异常", e);
            throw new RuntimeException("生成器失败", e);
        } finally {
            producer.flush();
            producer.close();
        }
    }

    private void sendBatch() {
        int count = Math.min(totalEventsPerSecond, BATCH_SIZE);
        for (int i = 0; i < count; i++) {
            try {
                BaseEvent event = eventFactory.createRandomEvent();
                String json = objectMapper.writeValueAsString(event);
                String topic = mapEventTypeToTopic(event.getEventType());
                String partitionKey = selectPartitionKey(event);

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, partitionKey, json);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        LOG.error("发送失败: topic={}, eventId={}", topic, event.getEventId(), exception);
                    }
                });

                totalEventsSent.incrementAndGet();
                totalBytesSent.addAndGet(json.getBytes().length);
            } catch (Exception e) {
                LOG.error("生成或序列化事件失败", e);
            }
        }
    }

    /**
     * 选择分区键：价格变更用 productId，点击用 eventId。
     */
    private String selectPartitionKey(BaseEvent event) {
        if (event instanceof ProductPriceEvent priceEvent) {
            return priceEvent.getProductId();
        }
        return event.getEventId();
    }

    private String mapEventTypeToTopic(String eventType) {
        return switch (eventType) {
            case "CLICK" -> "events.clicks";
            case "PRODUCT_PRICE" -> "events.product_price";
            default -> throw new IllegalArgumentException("未知事件类型: " + eventType);
        };
    }

    private boolean shouldContinue(long startTime) {
        if (durationSeconds < 0) {
            return true;
        }
        long elapsed = (System.currentTimeMillis() - startTime) / 1000;
        return elapsed < durationSeconds;
    }

    private Producer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        return new KafkaProducer<>(props);
    }

    private static String getRequiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("环境变量 " + name + " 未设置，无法启动");
        }
        return value.trim();
    }

    private static int parseTotalRate(String topicsConfig) {
        int total = 0;
        for (String part : topicsConfig.split(",")) {
            String[] kv = part.split(":");
            if (kv.length != 2) {
                throw new IllegalArgumentException("GENERATOR_TOPICS 格式错误: " + topicsConfig
                        + "，正确格式: orders:30,clicks:50,product_price:20");
            }
            total += Integer.parseInt(kv[1].trim());
        }
        if (total <= 0) {
            throw new IllegalArgumentException("GENERATOR_TOPICS 总速率必须大于 0");
        }
        return total;
    }

    public static void main(String[] args) {
        new KafkaDataGenerator().run();
    }
}
