# flink-cdc-playground

贴近生产的 **Flink CDC 3.x 实时数据同步练习项目**。

通过本项目，你可以完整练习一条实时数据湖链路：**PostgreSQL CDC → Kafka → Flink → Iceberg**，掌握 Flink DataStream 编程、维表关联（Broadcast + AsyncIO）、双流 JOIN、Exactly-Once、Checkpoint 恢复等核心技能。

## 项目特性

| 特性 | 说明 |
|------|------|
| 完整链路 | PG → Kafka → Flink → Iceberg 端到端跑通 |
| 声明式 CDC | Flink CDC 3.x Pipeline API（YAML 定义整库同步） |
| 编程式处理 | Flink DataStream Java API（清洗 / JOIN / 维表关联） |
| 维表关联 | Broadcast 广播流（小维表）+ AsyncIO + JDBC（中维表） |
| 双流 JOIN | 订单 + 支付按 order_id 关联，输出 DWD 宽表 |
| 死信队列 DLQ | 反序列化失败消息侧输出到 `dlq.cdc` topic |
| 生产级配置 | Checkpoint + RocksDB 增量 + 固定延迟重启 |
| 配置外置 | 全部环境变量注入，无硬编码、无回退默认值 |

## 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Apache Flink | 2.2.x | 流处理引擎 |
| Flink CDC | 3.7.0 | 变更数据捕获 + Pipeline API |
| PostgreSQL | 16 | CDC 源库 + 维表库 |
| Apache Kafka | 4.0.0（KRaft） | 中央数据总线 |
| Apache Iceberg | 1.12.0 | 数据湖表格式 |
| Java | 17 | 开发语言 |
| Maven | 3.9+ | 构建工具 |
| Docker Compose | v2 | 本地环境编排 |

## 整体架构

```
PostgreSQL(public) ──[Flink CDC Pipeline YAML]──> Kafka(cdc.*) ──┐
                                                                  ├──> Flink DWD Job ──> Iceberg(dwd.order_payment_wide)
Kafka 事件生成器 ───────────────────────────────> Kafka(events.*)┘
                                                                  ▲
                                              PostgreSQL(dim) ────┤
                                              (维表: Broadcast / AsyncIO)
```

**两条主链路：**

| 链路 | 实现方式 | 主类 |
|------|---------|------|
| PG → Kafka | Flink CDC Pipeline API（YAML 声明式） | `PostgresCdcToKafka` |
| Kafka → Iceberg | Flink DataStream API（Java 编程式） | `DwdOrderPaymentJob` |

## 快速开始

### 1. 启动 Docker 环境

```bash
cd docker
docker-compose up -d
```

| 服务 | 容器名 | 宿主机端口 | 说明 |
|------|--------|-----------|------|
| PostgreSQL | pg-cdc | 5433 | CDC 源库 + 维表库（wal_level=logical） |
| Kafka | kafka | 9092 | KRaft 单节点 |
| Kafka-UI | kafka-ui | 8888 | Kafka 可视化（http://localhost:8888） |

### 2. 配置环境变量

```bash
cp .env.example .env
# .env 中的配置已适配本地 Docker 环境，一般无需修改
```

### 3. 编译项目

```bash
mvn clean package -DskipTests -q
```

### 4. 生成测试数据

```bash
# 方式一：PostgreSQL 数据生成器（推荐，模拟业务表 INSERT/UPDATE/DELETE）
set -a && source .env && set +a
DG_CP="data-generator/target/classes:$(mvn dependency:build-classpath -pl data-generator -Dmdep.outputFile=/tmp/dg-cp.txt -q && cat /tmp/dg-cp.txt)"
java -cp "$DG_CP" \
  -Dlog4j2.configurationFile=file:$(pwd)/data-generator/src/main/resources/log4j2.properties \
  com.pipeline.generator.postgres.PostgresDataGenerator

# 方式二：Kafka 事件生成器（模拟业务事件流，可选）
java -cp "$DG_CP" \
  -Dlog4j2.configurationFile=file:$(pwd)/data-generator/src/main/resources/log4j2.properties \
  com.pipeline.generator.kafka.KafkaDataGenerator
```

> 生成器运行数秒后可 `Ctrl+C` 停止，已生成的数据会保留在 PG 中。

### 5. 启动 CDC 同步（PG → Kafka）

```bash
# 进入项目根目录，加载环境变量
set -a && source .env && set +a

# 构建 classpath 并运行（必须用 java -cp，不能用 mvn exec）
mvn dependency:build-classpath -pl flink-jobs -Dmdep.outputFile=/tmp/cp.txt -q
CP="flink-jobs/target/classes:$(cat /tmp/cp.txt)"

java --add-opens java.base/java.nio=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.util=ALL-UNNAMED \
  -cp "$CP" \
  -Dlog4j.configuration=file:$(pwd)/flink-jobs/src/main/resources/log4j.properties \
  com.pipeline.flink.cdc.PostgresCdcToKafka
```

验证：Kafka 出现 `cdc.orders`、`cdc.payments`、`cdc.base_region` 等 topic，且有消息。

### 6. 启动 DWD 处理（Kafka → Iceberg）

另开一个终端：

```bash
set -a && source .env && set +a
CP="flink-jobs/target/classes:$(cat /tmp/cp.txt)"

java --add-opens java.base/java.nio=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.util=ALL-UNNAMED \
  -cp "$CP" \
  -Dlog4j.configuration=file:$(pwd)/flink-jobs/src/main/resources/log4j.properties \
  com.pipeline.flink.kafka.DwdOrderPaymentJob
```

验证：`./warehouse/dwd/order_payment_wide/data/` 下出现 parquet 文件，`metadata/` 下有快照。

## 如何练习

本项目按里程碑拆分，建议按顺序练习，每步聚焦一个核心知识点：

| 步骤 | 练习内容 | 对应代码 | 核心知识点 |
|------|---------|---------|-----------|
| 1 | 环境搭建 + 数据生成 | `data-generator/`、`docker/` | Docker Compose、JDBC Batch、HikariCP |
| 2 | CDC 整库同步到 Kafka | `PostgresCdcToKafka.java`、`postgres-to-kafka.yaml` | Flink CDC Pipeline API、Debezium JSON |
| 3 | Kafka 消费 + 反序列化 | `CdcDeserializationSchema.java`、`CdcDeserializationSplitter.java` | KafkaSource、DeserializationSchema、侧输出 |
| 4 | 双流 JOIN | `OrderPaymentJoinFunction.java` | KeyedCoProcessFunction、ValueState、状态 TTL |
| 5 | 维表关联 | `RegionBroadcastJoin.java`、`UserAsyncIoJoin.java` | BroadcastState、AsyncDataStream、JDBC 异步查询 |
| 6 | 写入 Iceberg | `IcebergSinkFactory.java`、`FlinkWriteResultKryoSerializer.java` | Iceberg Catalog、Table API、Kryo 自定义序列化 |
| 7 | 生产增强 | `StreamEnvBase.java` | Checkpoint、RocksDB 增量、重启策略、DLQ |

> 每步的详细练习指引见 [docs/04-练习指引.md](docs/04-练习指引.md)。

## 项目结构

```
flink-cdc-playground/
├── data-generator/              # 数据生成器（Kafka + PostgreSQL）
│   └── src/main/java/com/pipeline/generator/
│       ├── kafka/               # Kafka 事件生成器
│       └── postgres/            # PG 业务数据生成器
├── flink-jobs/                  # Flink 作业
│   └── src/main/java/com/pipeline/flink/
│       ├── cdc/                 # CDC 同步（PG → Kafka）
│       ├── common/              # 公共工具（环境变量、序列化、Sink 工厂）
│       │   ├── serde/           # 反序列化器
│       │   └── annotation/      # 实体注解
│       └── kafka/               # Kafka 处理（→ Iceberg）
│           ├── enrichment/      # 维表关联（Broadcast + AsyncIO）
│           └── model/           # CDC 数据模型
├── docker/                      # Docker Compose 环境
│   └── postgres/init/           # PG 初始化脚本（建表 + CDC 账号）
├── sql/                         # DDL 脚本
│   ├── iceberg/                 # Iceberg 表结构
│   └── postgres/                # PG 源表 + 维表结构
├── docs/                        # 文档
│   ├── knowledge/               # 知识手册
│   └── code-reading/            # 代码阅读笔记
├── .env.example                 # 环境变量模板
└── pom.xml                      # 父 POM
```

## 文档导航

| 文档 | 说明 |
|------|------|
| [需求文档](docs/01-需求文档.md) | 项目需求与功能清单 |
| [架构说明](docs/02-架构说明.md) | 整体架构与链路设计 |
| [开发任务清单](docs/03-开发任务清单.md) | 任务拆分与进展跟踪 |
| [练习指引](docs/04-练习指引.md) | 按步骤的练习指南 |
| [运行手册](docs/05-运行手册.md) | 启动、验证、排查步骤 |
| [踩坑记录](docs/06-踩坑记录.md) | 实战中遇到的问题与解法 |
| [知识手册](docs/knowledge/) | Flink 核心概念讲解 |
| [代码阅读笔记](docs/code-reading/) | 关键类的逐行解读 |

## 常见问题

**Q：为什么用 `java -cp` 而不是 `mvn exec:java`？**
A：Flink CDC 的 `PipelineExecutor` 依赖 SPI 机制加载实现类，`mvn exec:java` 的类加载器隔离会导致 `ClassNotFoundException: SimpleUdfStreamOperatorFactory`。用 `java -cp` 把所有依赖放在同一个 classpath 下即可解决。

**Q：`--add-opens` 是做什么的？**
A：Java 17 对 `java.nio`、`java.util` 等模块做了强封装，Flink/Iceberg 内部需要通过反射访问 `ByteBuffer.hb` 等字段，不加会抛 `InaccessibleObjectException`。

**Q：Iceberg 数据写到哪里了？**
A：默认在 `./warehouse/`（由 `ICEBERG_WAREHOUSE_PATH` 控制）。DWD 表在 `./warehouse/dwd/order_payment_wide/`，包含 `data/`（parquet 文件）和 `metadata/`（快照）。

## 许可证

MIT
