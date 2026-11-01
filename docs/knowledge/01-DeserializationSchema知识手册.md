# DeserializationSchema 知识手册

> 适用于 Flink 2.x。阅读对象：Flink 初学者。
> 目标：理解 DeserializationSchema 的职责、接口方法、使用方式，并能独立编写自定义反序列化器。

***

## 一、概念与定位

### 1.1 是什么

`DeserializationSchema` 是 Flink 提供的**反序列化契约接口**，定义了"原始字节 → 业务对象"的转换规则。

Flink Source（如 KafkaSource）从外部系统拉取到的是二进制字节数组（`byte[]`），业务算子需要的是强类型的 Java 对象（如 `Order`、`Payment`）。`DeserializationSchema` 就是这两者之间的翻译层。

### 1.2 解决什么问题

将外部系统的原始字节数据，转换为 Flink 作业内部流转的 Java 对象，使下游算子可以直接操作结构化数据，无需关心字节解析细节。

### 1.3 在数据链路中的位置

```
Kafka broker → 网络拉取 → byte[] → DeserializationSchema → 业务对象 → 下游算子
```

| 阶段 | 数据形态         | 谁负责                   |
| -- | ------------ | --------------------- |
| 存储 | 二进制字节        | Kafka                 |
| 传输 | byte\[]      | KafkaSource           |
| 转换 | byte\[] → 对象 | DeserializationSchema |
| 计算 | Java 对象      | 业务算子                  |

### 1.4 与 SerializationSchema 的对应关系

| 接口                         | 方向          | 用途          |
| -------------------------- | ----------- | ----------- |
| `DeserializationSchema<T>` | byte\[] → T | Source 端：读取 |
| `SerializationSchema<T>`   | T → byte\[] | Sink 端：写入   |

两者对称，分别处理输入和输出的字节转换。

***

## 二、接口定义（Flink 2.x）

```java
public interface DeserializationSchema<T> extends Serializable {

    T deserialize(byte[] message) throws IOException;

    boolean isEndOfStream(T nextElement);

    TypeInformation<T> getProducedType();
}
```

| 方法                | 签名                                     | 作用          | 是否必须实现 |
| ----------------- | -------------------------------------- | ----------- | ------ |
| `deserialize`     | `T deserialize(byte[] message)`        | 字节转对象的核心逻辑  | 是      |
| `isEndOfStream`   | `boolean isEndOfStream(T nextElement)` | 判断输入流是否到达末尾 | 是      |
| `getProducedType` | `TypeInformation<T> getProducedType()` | 声明产出对象的类型信息 | 是      |

***

## 三、三个方法详解

### 3.1 `deserialize(byte[] message)`

**职责**：将一条 Kafka 消息的原始字节转换为目标对象。

| 输入                             | 输出           |
| ------------------------------ | ------------ |
| `byte[] message`：Kafka 消息的原始字节 | `T`：转换后的业务对象 |

**异常处理策略**有两种：

| 策略     | 做法                           | 后果            | 适用场景           |
| ------ | ---------------------------- | ------------- | -------------- |
| 抛异常    | `throw new IOException(...)` | 作业失败重启        | 数据质量要求高，不允许脏数据 |
| 返回失败标记 | 包装成功/失败结果，下游分流               | 作业继续，脏数据进 DLQ | 生产环境，容忍脏数据     |

### 3.2 `isEndOfStream(T nextElement)`

**职责**：通知 Flink 输入流是否已结束。

| 流类型               | 返回值            | 说明                  |
| ----------------- | -------------- | ------------------- |
| 有界流（文件、批量）        | 到达末尾时返回 `true` | Flink 收到 true 后停止读取 |
| 无界流（Kafka、Socket） | 固定返回 `false`   | 流永不结束，持续消费          |

Kafka 是无界流，所以 `isEndOfStream` 固定返回 `false`。

### 3.3 `getProducedType()`

**职责**：告诉 Flink 产出对象的类型信息，供 Flink 类型系统和序列化框架使用。

**为什么需要**：Flink 在状态存储、网络传输、算子链优化时，需要知道对象的具体类型来选择序列化器。如果类型信息缺失，会抛出 `NullPointerException` 或类型推导错误。

**常用写法**：

```java
@Override
public TypeInformation<T> getProducedType() {
    // TypeHint 匿名类用于在运行时保留泛型类型，避免泛型擦除
    return TypeInformation.of(new TypeHint<你的产出类型：T>() {});
}
```

> **泛型擦除**：Java 泛型在编译后会被擦除为 Object，运行时无法直接获取 `T` 的实际类型。`TypeHint` 通过匿名内部类的反射机制，在运行时恢复泛型类型信息。

### 3.4 骨架实现：`AbstractDeserializationSchema`

Flink 提供了 `DeserializationSchema` 的抽象骨架类 `AbstractDeserializationSchema<T>`，默认实现了 `isEndOfStream` 方法。

**继承关系：**

```
DeserializationSchema（接口）
        ↑ 继承
AbstractDeserializationSchema（抽象类，默认实现 isEndOfStream）
        ↑ 继承
你的自定义 Schema 类
```

#### 对比

| 维度 | `DeserializationSchema`（接口） | `AbstractDeserializationSchema`（抽象类） |
|------|-------------------------------|------------------------------------------|
| 类型 | interface | abstract class |
| 需要实现的方法 | 全部 3 个：`deserialize`、`isEndOfStream`、`getProducedType` | 只需要实现 `deserialize` 和 `getProducedType` |
| `isEndOfStream` | 必须自己实现 | 已默认实现，固定返回 `false` |
| 适用场景 | 需要自定义流结束判断（有界流）；或需要继承其他类（Java 单继承限制） | 无界流（Kafka 等），不需要改 `isEndOfStream` |
| 代码量 | 多 | 少（少写一个方法） |

#### `AbstractDeserializationSchema` 源码（简化）

```java
public abstract class AbstractDeserializationSchema<T> implements DeserializationSchema<T> {

    @Override
    public boolean isEndOfStream(T nextElement) {
        // 默认实现：无界流，永远不结束
        return false;
    }
}
```

#### 两种写法对比

**实现接口：**

```java
public class CdcDeserializationSchema<T>
        implements DeserializationSchema<DeserializationResult<CdcEnvelope<T>>> {

    @Override
    public DeserializationResult<CdcEnvelope<T>> deserialize(byte[] message) { ... }

    @Override
    public TypeInformation<...> getProducedType() { ... }

    // 必须自己写
    @Override
    public boolean isEndOfStream(DeserializationResult<CdcEnvelope<T>> nextElement) {
        return false;
    }
}
```

**继承抽象类：**

```java
public class CdcDeserializationSchema<T>
        extends AbstractDeserializationSchema<DeserializationResult<CdcEnvelope<T>>> {

    @Override
    public DeserializationResult<CdcEnvelope<T>> deserialize(byte[] message) { ... }

    @Override
    public TypeInformation<...> getProducedType() { ... }

    // 不用写，父类默认返回 false
}
```

#### 选型建议

| 场景 | 选哪个 |
|------|--------|
| Kafka 无界流，不需要判断结束 | `AbstractDeserializationSchema`（省代码） |
| 有界流，需要判断结束 | `DeserializationSchema`（自己实现 `isEndOfStream`） |
| 需要继承其他类（Java 单继承限制） | `DeserializationSchema`（接口可以多实现） |

***

## 四、内置实现对比

Flink 预置了常用的 `DeserializationSchema` 实现，开箱即用：

| 实现类                                 | 输入 → 输出              | 适用场景            |
| ----------------------------------- | -------------------- | --------------- |
| `SimpleStringSchema`                | byte\[] → String     | 调试、纯文本消息        |
| `JSONKeyValueDeserializationSchema` | byte\[] → ObjectNode | 通用 JSON，结构不固定   |
| `JsonNodeDeserializationSchema`     | byte\[] → JsonNode   | 同上，Flink 2.x 推荐 |
| `AvroDeserializationSchema`         | byte\[] → Avro 生成类   | Avro 二进制格式      |
| `ProtobufDeserializationSchema`     | byte\[] → Protobuf 类 | Protobuf 格式     |

选择原则：

- 消息格式是标准 JSON/Avro/Protobuf → 用内置实现
- 需要自定义错误处理或特殊解析逻辑 → 自定义实现

***

## 五、自定义实现步骤

以"将 Kafka JSON 消息转换为 `Order` 对象"为例。

### 第 1 步：确定泛型 T

目标类型是 `Order`，所以实现 `DeserializationSchema<Order>`。

### 第 2 步：实现 `deserialize`

```java
@Override
public Order deserialize(byte[] message) throws IOException {
    if (message == null || message.length == 0) {
        return null;
    }
    try {
        return MAPPER.readValue(message, Order.class);
    } catch (Exception e) {
        // 策略一：抛异常，作业失败
        throw new IOException("解析 Order 失败", e);
        // 策略二：返回 null，下游过滤（需配合 isEndOfStream）
    }
}
```

### 第 3 步：实现 `isEndOfStream`

Kafka 无界流，固定返回 `false`。

```java
@Override
public boolean isEndOfStream(Order nextElement) {
    return false;
}
```

### 第 4 步：实现 `getProducedType`

```java
@Override
public TypeInformation<Order> getProducedType() {
    return TypeInformation.of(Order.class);
}
```

### 第 5 步：注入 KafkaSource

```java
KafkaSource.<Order>builder()
    .setBootstrapServers("localhost:9092")
    .setTopics("orders")
    .setGroupId("my-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new OrderDeserializationSchema())
    .build();
```

***

## 六、本项目实例拆解（CdcDeserializationSchema）

项目中 [CdcDeserializationSchema.java](../flink-jobs/src/main/java/com/pipeline/flink/common/serde/CdcDeserializationSchema.java) 是一个带脏数据处理的自定义实现。

### 6.1 泛型嵌套结构

```java
DeserializationSchema<DeserializationResult<CdcEnvelope<T>>>
```

逐层拆解：

| 层级  | 类型                                      | 含义                                                       |
| --- | --------------------------------------- | -------------------------------------------------------- |
| 最内层 | `T`                                     | 业务实体（Order、Payment、Region）                               |
| 中层  | `CdcEnvelope<T>`                        | Debezium CDC 信封，包含 `op`（操作类型）、`before`（变更前）、`after`（变更后） |
| 外层  | `DeserializationResult<CdcEnvelope<T>>` | 结果包装层，区分成功/失败                                            |

### 6.2 为什么要包 `DeserializationResult`

`deserialize` 方法签名不允许"部分成功"——要么返回对象，要么抛异常。但生产环境需要：

- 解析成功的消息正常下发
- 解析失败的消息不中断作业，而是路由到 DLQ（死信队列）

`DeserializationResult` 就是这个目的：

```
解析成功 → DeserializationResult.success(envelope)
解析失败 → DeserializationResult.failure(原始JSON字符串)
```

下游 `CdcDeserializationSplitter` 根据 success/failure 分流：成功进主流，失败进侧输出（DLQ）。

### 6.3 Jackson 泛型反序列化

由于 `CdcEnvelope<T>` 是泛型类，Jackson 无法直接通过 `CdcEnvelope.class` 推断 `T` 的类型，需要用 `TypeFactory` 构建参数化类型：

```java
TypeFactory typeFactory = MAPPER.getTypeFactory();
var envelopeType = typeFactory.constructParametricType(CdcEnvelope.class, entityClass);
CdcEnvelope<T> envelope = MAPPER.readValue(message, envelopeType);
```

`entityClass` 是构造函数传入的业务实体 Class（如 `Order.class`），运行时告诉 Jackson `T` 的实际类型。

### 6.4 失败时保留原始 JSON

解析失败时，将原始字节转为 UTF-8 字符串保留在 `DeserializationResult.failure(raw)` 中，供下游写入 DLQ topic，便于事后排查和重放。

***

## 七、常见问题与坑

### 7.1 `getProducedType` 泛型擦除

**问题**：直接写 `TypeInformation.of(Class<T>)` 无法通过编译，因为 `T` 在运行时被擦除。

**解决**：使用 `TypeHint` 匿名内部类：

```java
// 错误
return TypeInformation.of((Class<T>) T.class);

// 正确
return TypeInformation.of(new TypeHint<DeserializationResult<CdcEnvelope<T>>>() {});
```

### 7.2 漏实现 `isEndOfStream`

**问题**：Flink 2.x 中 `isEndOfStream` 是接口方法，不实现会编译报错。

**解决**：无界流固定返回 `false`。

### 7.3 静态 ObjectMapper 的线程安全

**问题**：`ObjectMapper` 实例创建成本高，是否可以共享？

**结论**：`ObjectMapper` 本身线程安全，可以声明为 `static final` 单例共享。但 `ObjectReader`/`ObjectWriter` 是可变的，不能共享。项目中使用静态 `MAPPER` 单例是正确做法。

### 7.4 反序列化异常的处理选择

| 选择              | 后果                 | 建议             |
| --------------- | ------------------ | -------------- |
| 抛 `IOException` | 作业失败，重启后重新消费       | 仅用于数据格式必须正确的场景 |
| 返回 `null`       | 下游需要过滤 null，可能丢失数据 | 不推荐            |
| 包装失败标记          | 脏数据进 DLQ，作业继续      | **生产环境推荐**     |

***

## 八、与其他反序列化方式的对比

| 方式                      | 代码量 | 灵活性 | 错误处理      | 适用场景                   |
| ----------------------- | --- | --- | --------- | ---------------------- |
| `DeserializationSchema` | 中   | 高   | 自定义       | 需要自定义解析逻辑或 DLQ         |
| Flink SQL `format`      | 少   | 低   | 内置（跳过或失败） | 标准 JSON/Avro/CSV       |
| 直接 `map` 转换             | 中   | 中   | 自定义       | 简单场景，Source 已输出 String |

选择建议：

- 用 Flink SQL → 直接用 `format = 'json'` 等声明式配置
- 用 DataStream API 且需要精细控制 → 实现 `DeserializationSchema`
- 消息已是 String → 用 `SimpleStringSchema` + `map` 解析

***

## 附录：核心术语表

| 术语                       | 含义                                          |
| ------------------------ | ------------------------------------------- |
| 反序列化（Deserialization）    | 将字节/字符串还原为结构化对象的过程                          |
| 泛型擦除（Type Erasure）       | Java 泛型在编译后被替换为 Object，运行时丢失类型信息            |
| TypeHint                 | Flink 提供的工具类，通过匿名内部类在运行时恢复泛型类型              |
| DLQ（Dead Letter Queue）   | 死信队列，存放无法正常处理的消息                            |
| CDC（Change Data Capture） | 变更数据捕获，捕获数据库的 INSERT/UPDATE/DELETE 事件       |
| Debezium Envelope        | Debezium CDC 消息的标准结构，包含 op、before、after 等字段 |

