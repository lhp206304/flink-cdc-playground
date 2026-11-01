# 阅读 FlinkWriteResultKryoSerializer 的问题记录

> 阅读对象：[FlinkWriteResultKryoSerializer.java](../../flink-jobs/src/main/java/com/pipeline/flink/common/FlinkWriteResultKryoSerializer.java)
> 记录日期：2026-10-29
> 阅读目标：理解自定义 Kryo 序列化器的写法，以及 `final` 字段、二进制流读写的原理。

---

## 问题一：为什么 `writeResultSerializer` 字段要用 `final`？

### 1.1 `final` 修饰成员变量的含义

`final` 修饰成员变量时，表示**这个引用一旦赋值就不能再指向别的对象**。

注意锁定的是**引用**（变量名指向哪个对象），不是**对象内容**。对象内部的字段仍然可以修改。

### 1.2 为什么这里用 `final`

```java
private final WriteResultSerializer writeResultSerializer = new WriteResultSerializer();
```

这个序列化器在对象创建时实例化，之后整个生命周期都不需要换。加 `final` 表达三个意图：

| 作用 | 说明 |
|------|------|
| 防误改 | 编译器禁止后续重新赋值，避免被覆盖成 null 或别的实例 |
| 线程安全 | JMM 保证 `final` 字段的写入对其他线程可见 |
| 语义清晰 | 告诉读代码的人：这个依赖固定不变，不会动态切换 |

对比不用 `final` 的情况：

```java
// 不用 final：可以被重新赋值，有风险
private WriteResultSerializer serializer = new WriteResultSerializer();
serializer = null;            // 编译器不拦，运行时 NPE
serializer = new OtherSer();  // 编译器不拦，行为被偷偷改掉

// 用 final：编译器直接报错
private final WriteResultSerializer serializer = new WriteResultSerializer();
serializer = null;            // 编译错误：cannot assign a value to final variable
```

### 1.3 `final` 的作用域

`final` 只约束**同一个实例内**的引用，不跨实例。

```
实例 A：a.writeResultSerializer → WriteResultSerializer 对象 #1（锁定，不可改）
实例 B：b.writeResultSerializer → WriteResultSerializer 对象 #2（锁定，不可改）
```

每个 `FlinkWriteResultKryoSerializer` 实例都会 `new` 一个自己的 `WriteResultSerializer`，彼此独立。

### 1.4 `final` 字段的可见性保证（JMM 规范）

在多线程环境下，线程 A 修改了某个普通字段的值，线程 B 不一定能立刻看到新值（因为 CPU 有缓存、寄存器、指令重排）。这就是**可见性问题**。

JMM（Java 内存模型）对 `final` 有特殊承诺：**对象的 `final` 字段在构造方法中完成赋值后，其他线程拿到这个对象引用时，一定能看到 `final` 字段的正确值**，不会读到半初始化的 null。

| 字段类型 | 其他线程看到的值 |
|---------|----------------|
| `final` 字段 | 保证是构造方法里赋的值 |
| 普通字段 | 可能是默认值（null / 0），不保证 |

原因：JMM 禁止把 `final` 字段的写操作重排到构造方法之外，保证对象引用对外可见时，`final` 字段已经初始化完毕。

### 1.5 字段声明处初始化算"构造方法中赋值"吗？

**算。** 字段初始化器（`= new WriteResultSerializer()`）编译后会被插入到构造方法中执行。

源代码写法：

```java
public class FlinkWriteResultKryoSerializer extends Serializer<FlinkWriteResult> {
    private final WriteResultSerializer writeResultSerializer = new WriteResultSerializer();
    // 没有显式构造方法，编译器自动生成无参构造方法
}
```

编译器生成的等价代码：

```java
public class FlinkWriteResultKryoSerializer extends Serializer<FlinkWriteResult> {
    private final WriteResultSerializer writeResultSerializer;

    public FlinkWriteResultKryoSerializer() {
        super();                                                // ① 先调用父类构造方法
        this.writeResultSerializer = new WriteResultSerializer(); // ② 字段初始化器被插到这里
    }
}
```

| 维度 | 字段声明处初始化 | 构造方法中显式赋值 |
|------|----------------|------------------|
| 执行时机 | 构造方法执行期间 | 构造方法执行期间 |
| `final` 可见性保证 | ✅ 享有 | ✅ 享有 |

两种写法在运行时行为完全一致。

---

## 问题二：`write` / `read` 方法该写什么？读写靠什么区分？

### 2.1 怎么知道 `write` 该写什么

由**被序列化对象的字段结构**决定。`FlinkWriteResult` 有两个字段：

```java
public class FlinkWriteResult {
    private final long checkpointId;        // 字段 1：long 类型
    private final WriteResult writeResult;  // 字段 2：复杂对象
}
```

有几个字段就写几段，字段类型决定用哪个 `write` 方法。

### 2.2 定长类型：固定字节数，直接读写

`checkpointId` 是 `long`，固定占 **8 字节**。

```java
// 写
output.writeLong(object.checkpointId());   // 向输出流写入 8 字节

// 读
long checkpointId = input.readLong();      // 从输入流读取 8 字节
```

Java 基本类型的固定字节数：

| 类型 | 字节数 | Kryo 方法 |
|------|-------|----------|
| `boolean` | 1 | `writeBoolean` / `readBoolean` |
| `byte` | 1 | `writeByte` / `readByte` |
| `short` | 2 | `writeShort` / `readShort` |
| `int` | 4 | `writeInt` / `readInt` |
| `long` | 8 | `writeLong` / `readLong` |
| `float` | 4 | `writeFloat` / `readFloat` |
| `double` | 8 | `writeDouble` / `readDouble` |
| `char` | 2 | `writeChar` / `readChar` |

### 2.3 变长类型：必须用长度前缀

`writeResult` 是 `WriteResult` 对象，序列化成 `byte[]` 后长度不固定。如果不先记录长度，`read` 时无法判断"到哪里为止是这个字段的内容"。

**写端**：

```java
byte[] bytes = writeResultSerializer.serialize(object.writeResult());  // ① 对象 → 字节数组
output.writeInt(bytes.length);                                          // ② 先写数组长度（4 字节）
output.writeBytes(bytes);                                               // ③ 再写数组内容
```

**读端**：

```java
int len = input.readInt();           // ① 先读长度（4 字节），知道接下来要读多少字节
byte[] bytes = input.readBytes(len); // ② 按长度读取内容
```

### 2.4 读指针（offset）自动推进

`Input` 对象内部维护一个**读指针**（offset，即"当前读到第几个字节"）。每次调用 `readXxx` 方法，指针自动向后移动对应字节数，下一次读取从新位置开始。

```
初始指针位置：0
┌─────────────────────────────────────────────────────────────────┐
│ [checkpointId 8字节] [长度len 4字节] [WriteResult内容 len字节]    │
└─────────────────────────────────────────────────────────────────┘
  ↑
  offset=0

步骤① input.readLong()      → 读取 [0, 8)，指针移动到 8
步骤② input.readInt()       → 读取 [8, 12)，指针移动到 12
步骤③ input.readBytes(len)  → 读取 [12, 12+len)，指针移动到 12+len
```

**关键点**：整个过程没有任何"标志位"或"分隔符"来标记字段边界。读写双方靠的是**事先约定好的格式**（写什么类型、按什么顺序、变长字段是否带长度前缀）。只要 `write` 和 `read` 严格对称，指针就不会错位；一旦不对称（比如写了 `writeInt` 却用 `readLong` 读），指针就会错位，后续所有字段全部读错。

### 2.5 对应关系总表

| 字段 | 类型 | 定长/变长 | 写端代码 | 读端代码 | 字节布局 |
|------|------|----------|---------|---------|---------|
| `checkpointId` | `long` | 定长（8 字节） | `output.writeLong(...)` | `input.readLong()` | 8 字节 |
| `writeResult` | `WriteResult` | 变长 | `writeInt(len)` + `writeBytes(bytes)` | `readInt()` + `readBytes(len)` | 4 字节长度 + len 字节内容 |

---

## 一句话规则总结

> 序列化自定义对象时，**按字段顺序逐个写入**：基本类型直接用对应的 `writeXxx`（定长，自动处理）；对象/数组等变长内容先转成 `byte[]`，再以"长度前缀 + 内容"的方式写入。读取时按**完全相同**的顺序和类型调用 `readXxx`，读指针会自动推进到正确位置。
