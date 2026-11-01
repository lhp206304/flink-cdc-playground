# ObjectMapper 知识手册

> 适用于 Jackson 2.x。阅读对象：Java 初学者。
> 目标：理解 ObjectMapper 的职责、核心 API、配置项，并能处理泛型反序列化等常见场景。

---

## 一、概念与定位

### 1.1 是什么

`ObjectMapper` 是 Jackson 库的**核心类**，负责 JSON 与 Java 对象之间的双向转换。

Jackson 是 Java 生态最主流的 JSON 处理库，Spring Boot 默认集成。

### 1.2 解决什么问题

将结构化的 JSON 数据还原为 Java 对象（反序列化），或将 Java 对象输出为 JSON（序列化），使业务代码无需手动解析 JSON 字符串。

### 1.3 在本项目中的位置

[CdcDeserializationSchema.java](../../flink-jobs/src/main/java/com/pipeline/flink/common/serde/CdcDeserializationSchema.java) 中使用 ObjectMapper 将 Kafka 的 Debezium CDC JSON 消息解析为 `CdcEnvelope<T>` 对象。

```
Kafka byte[] → ObjectMapper.readValue() → CdcEnvelope<T> → 下游算子
```

---

## 二、核心能力

| 方向 | 方法 | 输入 → 输出 |
|------|------|------------|
| 反序列化 | `readValue(...)` | JSON 字符串/字节 → Java 对象 |
| 序列化 | `writeValueAsString(...)` | Java 对象 → JSON 字符串 |

两个方向是对称的，分别对应"读"和"写"。

---

## 三、创建与配置

### 3.1 基本创建

```java
ObjectMapper mapper = new ObjectMapper();
```

### 3.2 链式配置项

通过 `configure(feature, state)` 方法调整行为：

| 配置项 | 默认值 | 设为 false 的效果 |
|--------|--------|------------------|
| `FAIL_ON_UNKNOWN_PROPERTIES` | true | JSON 中有对象没有的字段时不报错，直接忽略 |
| `FAIL_ON_EMPTY_BEANS` | true | 空对象（无字段）不报错 |
| `WRITE_DATES_AS_TIMESTAMPS` | true | 日期序列化为 ISO 字符串而非时间戳数字 |

### 3.3 `findAndRegisterModules()`

自动扫描 classpath 下的 Jackson 扩展模块并注册。

常见模块：
- `jackson-datatype-jsr310`：支持 Java 8 时间类型（`LocalDateTime`、`LocalDate`）
- `jackson-datatype-jdk8`：支持 `Optional`

不调用此方法时，`LocalDateTime` 等类型会解析失败。

### 3.4 为什么用 `static final` 单例

```java
private static final ObjectMapper MAPPER = new ObjectMapper()...;
```

| 原因 | 说明 |
|------|------|
| 创建成本高 | ObjectMapper 内部缓存序列化器/反序列化器，首次创建开销大 |
| 线程安全 | ObjectMapper 本身线程安全，可多线程共享 |
| 节省内存 | 全局一份，避免每个实例重复创建 |

---

## 四、反序列化详解

### 4.1 简单类型

```java
Order order = mapper.readValue(jsonBytes, Order.class);
```

直接传入目标类的 Class 对象即可。

### 4.2 泛型类型：为什么需要 TypeFactory

**判断标准：目标类型是否带泛型参数。**

| 目标类型 | 是否需要 constructParametricType | 写法 |
|---------|--------------------------------|------|
| `Order`（普通类） | 否 | `readValue(json, Order.class)` |
| `CdcEnvelope<T>`（带泛型） | 是 | `readValue(json, constructParametricType(CdcEnvelope.class, T.class))` |
| `List<T>`（带泛型） | 是 | `readValue(json, constructCollectionType(List.class, T.class))` |
| `Map<String, T>`（带泛型） | 是 | `readValue(json, constructMapType(Map.class, String.class, T.class))` |

**一句话记忆：普通类传 `.class`，带泛型用 `TypeFactory` 构建。**

**问题**：`CdcEnvelope<T>` 是泛型类，`readValue(json, CdcEnvelope.class)` 在运行时只知道外层是 `CdcEnvelope`，不知道 `T` 是什么。Jackson 会把 `after` 字段解析成 `LinkedHashMap` 而非 `Order`。

**原因**：Java 泛型擦除——编译后 `T` 被替换为 `Object`，运行时无法获取 `T` 的实际类型。

**解决**：用 `TypeFactory` 显式构建参数化类型：

```java
TypeFactory typeFactory = mapper.getTypeFactory();
JavaType envelopeType = typeFactory.constructParametricType(CdcEnvelope.class, Order.class);
CdcEnvelope<Order> envelope = mapper.readValue(jsonBytes, envelopeType);
```

`constructParametricType(外层类, 内层类型...)` 告诉 Jackson 泛型参数的实际类型。

### 4.3 集合类型

```java
JavaType listType = typeFactory.constructCollectionType(List.class, Order.class);
List<Order> orders = mapper.readValue(jsonBytes, listType);
```

### 4.4 本项目实例

```java
TypeFactory typeFactory = MAPPER.getTypeFactory();
var envelopeType = typeFactory.constructParametricType(CdcEnvelope.class, entityClass);
CdcEnvelope<T> envelope = MAPPER.readValue(message, envelopeType);
```

`entityClass` 是构造函数传入的业务实体 Class（如 `Order.class`），运行时注入泛型信息。

---

## 五、序列化详解

### 5.1 基本用法

```java
String json = mapper.writeValueAsString(order);
```

### 5.2 常用注解

| 注解 | 作用 | 示例 |
|------|------|------|
| `@JsonProperty("name")` | 字段重命名 | JSON 中用 `name` 而非 Java 字段名 |
| `@JsonIgnore` | 忽略单个字段 | 不序列化/反序列化该字段 |
| `@JsonIgnoreProperties(ignoreUnknown = true)` | 类级别忽略未知字段 | 等价于全局 `FAIL_ON_UNKNOWN_PROPERTIES=false` |
| `@JsonFormat(pattern = "yyyy-MM-dd")` | 日期格式 | 指定日期输出格式 |

示例：

```java
@JsonIgnoreProperties(ignoreUnknown = true)
public class Region {
    @JsonProperty("region_code")
    private String regionCode;

    @JsonIgnore
    private String internalField;
}
```

---

## 六、线程安全

| 对象 | 线程安全 | 能否共享 |
|------|---------|---------|
| `ObjectMapper` | 是 | 可以全局共享 |
| `ObjectReader` | 否 | 不可共享 |
| `ObjectWriter` | 否 | 不可共享 |

`ObjectReader` 和 `ObjectWriter` 是从 `ObjectMapper` 派生的，持有可变配置状态，多线程使用会相互干扰。

**正确用法**：共享 `ObjectMapper`，按需创建 `ObjectReader`/`ObjectWriter`。

---

## 七、常见问题与坑

| 问题 | 原因 | 解决 |
|------|------|------|
| 未知字段报错 | 默认 `FAIL_ON_UNKNOWN_PROPERTIES=true` | 设为 false，或类上加 `@JsonIgnoreProperties(ignoreUnknown = true)` |
| 泛型字段变成 `LinkedHashMap` | 泛型擦除，Jackson 不知道内层类型 | 用 `TypeFactory.constructParametricType` 构建参数化类型 |
| `LocalDateTime` 解析失败 | 缺少 Java Time 模块 | 调用 `findAndRegisterModules()` 或显式注册 `JavaTimeModule` |
| 日期输出为时间戳数字 | 默认 `WRITE_DATES_AS_TIMESTAMPS=true` | 设为 false，或字段加 `@JsonFormat` |

---

## 八、与其他 JSON 库对比

| 库 | 特点 | 适用场景 |
|----|------|---------|
| Jackson | 功能强、性能好、生态完善、Spring Boot 默认 | **生产环境主流推荐** |
| Fastjson | API 简单、速度快，但历史安全漏洞多 | 不推荐生产环境使用 |
| Gson | API 简洁、配置少 | 小型项目或对 Jackson 不熟悉的团队 |

---

## 九、本项目实例拆解

### 9.1 完整代码

```java
private static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .findAndRegisterModules();
```

### 9.2 逐项说明

| 配置 | 为什么需要 |
|------|-----------|
| `FAIL_ON_UNKNOWN_PROPERTIES = false` | Debezium CDC 消息包含 `source`、`ts_ms`、`transaction` 等字段，而 `CdcEnvelope` 只定义了 `op`、`before`、`after`。不忽略会导致解析失败 |
| `findAndRegisterModules()` | `CdcEnvelope` 的 `after` 是业务实体，其中包含 `LocalDateTime`（如 `created_at`、`updated_at`），需要 Java Time 模块支持 |

### 9.3 泛型反序列化

```java
TypeFactory typeFactory = MAPPER.getTypeFactory();
var envelopeType = typeFactory.constructParametricType(CdcEnvelope.class, entityClass);
CdcEnvelope<T> envelope = MAPPER.readValue(message, envelopeType);
```

`entityClass`（如 `Order.class`）由构造函数传入，运行时告诉 Jackson `CdcEnvelope<T>` 中 `T` 的实际类型，使 `after` 字段正确解析为目标业务对象。

---

## 附录：核心术语表

| 术语 | 含义 |
|------|------|
| 序列化（Serialization） | Java 对象 → JSON 字节的过程 |
| 反序列化（Deserialization） | JSON 字节 → Java 对象的过程 |
| 泛型擦除（Type Erasure） | Java 泛型在编译后被替换为 Object，运行时丢失类型信息 |
| TypeFactory | Jackson 提供的类型工厂，用于构建复杂泛型类型 |
| Module | Jackson 扩展模块，提供特定类型（如 Java 8 时间类型）的序列化支持 |
| JavaType | Jackson 内部表示的类型信息，包含泛型参数 |
