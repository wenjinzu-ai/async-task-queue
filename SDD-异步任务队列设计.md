# 软件设计文档（SDD）：异步任务队列

| 项目 | 内容 |
| --- | --- |
| 文档名称 | 异步任务队列 软件设计文档（SDD） |
| 版本 | v1.0 |
| 对应需求 | Java 开发工程师笔试题-异步任务队列-v1.2 |
| 技术栈 | Java 25 / Spring Boot 4.2 / Spring Data JPA / H2（文件持久化） / JDK 并发原语 |
| 文档状态 | 设计稿 |

---

## 1. 引言

### 1.1 目的

本文档面向「异步任务队列」笔试题的实现，给出从需求到代码的完整设计，覆盖并发编程、线程池治理、有界队列与背压、任务持久化、失败重试与幂等、优雅停机、可观测性等考察点，作为后续编码与面试深挖的依据。

### 1.2 范围

- 自行实现**有界优先级阻塞队列**（不引入任何队列中间件，基于 `Lock` / `Condition`）。
- 任务「接收 → 排队 → 处理 → 结果可查」完整链路。
- 任务持久化到 H2 文件库，保证入队不丢失、可恢复。
- 失败重试（次数上限 + 指数退避）、死信、幂等。
- 优雅停机、指标暴露。
- 不包含：真实对接外部 AI 服务（耗时用本地计算/sleep 模拟）、分布式部署（单机进程内队列）。

### 1.3 读者

开发工程师、面试评审者。

### 1.4 术语

| 术语 | 含义 |
| --- | --- |
| 任务（Task） | 一次异步执行单元，有唯一 `taskId` 与业务键 `bizKey` |
| 有界优先级队列 | 自行实现的、容量固定、按优先级出队的阻塞队列 |
| 背压（Backpressure） | 队列满时对提交方的明确反馈/降级动作 |
| 死信（Dead Letter） | 超过重试上限、不再自动重试的失败任务 |
| 不丢失边界 | 任务状态被持久化确认的时刻，此后即使进程崩溃也可恢复 |

---

## 2. 需求概述

对应试题「一、核心需求」与「二、功能描述」：

1. 选定一个异步任务场景并建模（任务类型、输入输出、耗时特征、失败概率）。
2. 完整链路：接收、排队、处理、结果可查。
3. 任务一旦入队不丢失。
4. 内存队列必须有界，容量可配置/自适应，超限有明确背压策略。
5. 不引入队列中间件，队列自行实现。
6. 任务支持优先级。
7. 提交快速返回；明确「接收成功」持久化边界；重复提交按规则处理（同业务键返回同 taskId 或拒绝）。
8. 失败重试（次数上限 + 退避），超限进入死信；重试不放大队列压力。
9. 按 taskId 查询状态/结果，按状态统计数量。
10. 优雅停机：停收新任务、内存未处理任务安全回写、线程池限时收敛。
11. 可观测性：队列长度/容量、活跃线程数、成功/失败/重试计数、平均排队与处理时长。

---

## 3. 场景与任务模型

### 3.1 选定场景：AI 文生图任务异步队列

面向 AI 应用方向，选定**文生图（Text-to-Image）异步生成**场景：用户提交一段提示词（prompt），系统异步调用模型生成图片，完成后返回图片访问地址。

选择理由：贴合「AI 应用方向」；典型的**耗时不确定 + IO 等待为主**的任务，天然需要异步化与排队；生成有失败概率（模型超时、内容审核拒绝），适合考察重试与失败分类。

### 3.2 任务结构

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `taskId` | String (UUID) | 任务唯一标识，提交即返回 |
| `bizKey` | String | 业务幂等键（如 `userId + promptHash`），唯一 |
| `taskType` | String | 任务类型，此处固定 `TEXT_TO_IMAGE` |
| `priority` | int | 优先级，数值越大越先出队（默认 5，范围 0~9） |
| `payload` | String (JSON) | 业务输入：`prompt`、`width`、`height`、`style` 等 |
| `status` | enum | 见 3.4 状态机 |
| `result` | String (JSON) | 业务输出：图片 URL、耗时、模型版本等（存于结果表） |
| `failReason` | String | 失败原因（最近一次） |
| `retryCount` | int | 已重试次数 |
| `maxRetry` | int | 最大重试次数（默认 3） |
| `nextRetryAt` | timestamp | 下次可重试时间（退避后） |
| `heartbeatAt` | timestamp | 处理中心跳时间，worker 定期刷新，用于精确判断任务是否真的失联（见 §6.7） |
| `enqueuedAt` / `startedAt` / `finishedAt` | timestamp | 排队/开始/完成时间，用于统计时长 |
| `createdAt` / `updatedAt` | timestamp | 记录时间 |

### 3.3 耗时特征 → 并发模型

- **耗时特征**：IO 等待为主（调用模型推理服务，单次 3~30s，P99 可达 60s），夹杂少量 CPU（payload 序列化、图片元信息处理）。属**二者混合、IO 占主导**。
- **失败概率**：模型服务波动 ~5%（可重试的系统/业务异常）；内容审核拒绝 ~2%（不可重试的业务失败）。
- **并发模型决策**：
  - IO 占主导 → 工作线程数可大于 CPU 核数，采用「核心线程数 = 2 × CPU 核数」起步，可配置。
  - 任务耗时长、长尾明显 → 必须有界队列 + 背压，避免无限堆积导致 OOM 与雪崩。
  - 优先级 → 高优先级任务（如付费用户）应插队，采用优先级队列。

### 3.4 任务状态机

```
        提交落库                 入内存队列              工作线程取走
PENDING ────────► QUEUED ────────────────────► PROCESSING ──────► SUCCESS
   │                │                            │
   │                │                            ├─ 可重试失败 ──► RETRY_WAITING ──(到期且未超限)──► QUEUED
   │                │                            │                      │
   │                │                            │                      └─ (到期时已达 maxRetry) ──► DEAD_LETTER
   │                │                            └─ 不可恢复 ─────────────────────────────────────► FAILED
   │                │
   └─ 重启回填 ──────┘
```

转移条件（**所有转移均由条件 UPDATE 守卫保证原子性，见 §6.9**）：
- `PENDING → QUEUED`：提交入队 / 启动回填 / 背压缓解后回填。
- `QUEUED → PROCESSING`：worker `take` 后抢到执行权（条件 UPDATE 返回 1）。
- `PROCESSING → SUCCESS`：处理成功，写结果表与改状态同一事务。
- `PROCESSING → RETRY_WAITING`：可重试失败且 `retryCount < maxRetry`，计算退避写入 `nextRetryAt`。
- `PROCESSING → DEAD_LETTER`：可重试失败但已达 `maxRetry`（超限即死信，不再等待）。
- `PROCESSING → FAILED`：不可恢复失败。
- `RETRY_WAITING → QUEUED`：`RetryScheduler` 扫描到 `nextRetryAt` 到期并抢到入队权。
- `PROCESSING → （恢复）RETRY_WAITING/QUEUED`：启动时心跳过期判定为失联，回收重试。

状态枚举：`PENDING`（已落库、未入内存队列）、`QUEUED`（在内存队列中排队）、`PROCESSING`（处理中）、`RETRY_WAITING`（退避等待重试）、`SUCCESS`、`FAILED`、`DEAD_LETTER`（超过重试上限的终态，可人工补偿）。

---

## 4. 总体架构设计

### 4.1 分层架构

```
┌─────────────────────────────────────────────────────────┐
│  API 层 (Controller)                                     │
│  POST /tasks   GET /tasks/{id}   GET /tasks/stats        │
├─────────────────────────────────────────────────────────┤
│  应用服务层 (Service)                                     │
│  TaskService(提交/查询)   TaskDispatcher(入队/回填)        │
│  RetryScheduler(重试调度)   MetricsService(指标)          │
├─────────────────────────────────────────────────────────┤
│  领域/并发层                                              │
│  BoundedPriorityBlockingQueue (自行实现)                  │
│  TaskExecutor (线程池封装)   TaskHandler (任务执行器)      │
│  BackpressureStrategy (背压策略)                          │
├─────────────────────────────────────────────────────────┤
│  持久层 (Repository / JPA)                                │
│  TaskRepository  TaskResultRepository                     │
├─────────────────────────────────────────────────────────┤
│  H2 文件数据库 (task 状态表 + task_result 结果表，分离)     │
└─────────────────────────────────────────────────────────┘
```

### 4.2 核心链路时序

**正常提交与处理：**

```
Client → Controller → TaskService.submit(payload)
                          │
                          ├─ 1. 幂等检查(bizKey) ── 已存在则返回原 taskId
                          ├─ 2. 写 task 表(status=PENDING)  ← 「接收成功」持久化边界
                          ├─ 3. 入内存队列(BoundedPriorityBlockingQueue)
                          │      ├─ 成功 → status=QUEUED, enqueuedAt=now
                          │      └─ 失败(满) → 触发背压策略(见 6.3)
                          └─ 返回 taskId  (快速返回)

TaskExecutor(工作线程) ──take()──► 队列取任务
                          ├─ status=PROCESSING, startedAt=now
                          ├─ TaskHandler 执行业务(sleep/本地计算模拟)
                          ├─ 成功 → 写 result 表, status=SUCCESS
                          └─ 失败 → 分类(见 6.5) → 重试 / FAILED / DEAD_LETTER
```

**恢复链路（启动 / 停机回填）：**

```
启动时 RecoveryService：
  - 扫描 status=PENDING/QUEUED 的任务 → 重新入内存队列
  - 扫描 status=PROCESSING 且超时的任务 → 标记重试
  - 扫描 status=RETRY_WAITING 且已到 nextRetryAt → 入队
```

---

## 5. 数据模型设计

H2 文件持久化模式，DDL 随项目提供（`schema.sql` 或 JPA 自动建表 + `schema.sql` 兜底），启动自动建表。**任务状态表与结果表分离**，便于查询与清理（结果表可定期归档/删除，不影响状态流转）。

### 5.1 task 表（任务状态表）

```sql
CREATE TABLE IF NOT EXISTS task (
    task_id        VARCHAR(64)  PRIMARY KEY,
    biz_key        VARCHAR(255) NOT NULL,
    task_type      VARCHAR(64)  NOT NULL,
    priority       INT          NOT NULL DEFAULT 5,
    payload        CLOB,
    status         VARCHAR(32)  NOT NULL,
    fail_reason    VARCHAR(1024),
    retry_count    INT          NOT NULL DEFAULT 0,
    max_retry      INT          NOT NULL DEFAULT 3,
    next_retry_at  TIMESTAMP,
    heartbeat_at   TIMESTAMP,
    enqueued_at    TIMESTAMP,
    started_at     TIMESTAMP,
    finished_at    TIMESTAMP,
    created_at     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_task_biz_key UNIQUE (biz_key)
);
CREATE INDEX IF NOT EXISTS idx_task_status ON task (status);
CREATE INDEX IF NOT EXISTS idx_task_next_retry ON task (status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_task_heartbeat ON task (status, heartbeat_at);
```

### 5.2 task_result 表（任务结果表，与状态表分离）

```sql
CREATE TABLE IF NOT EXISTS task_result (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_id       VARCHAR(64)   NOT NULL,
    result_data   CLOB,
    success       BOOLEAN       NOT NULL,
    created_at    TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_result_task_id UNIQUE (task_id)
);
CREATE INDEX IF NOT EXISTS idx_result_task ON task_result (task_id);
```

设计说明：
- **数据访问选型声明**：试题技术要求列出「Spring-data-jpa、MyBatis」两个选项，本项目选用 **Spring Data JPA** 作为唯一数据访问层（`@Modifying` + `@Query` 实现 §6.9 的条件 UPDATE 守卫），**不引入 MyBatis**。理由：① 二者功能在此场景下完全等价，引入两套 ORM 徒增依赖与心智负担；② JPA 的实体映射与 `schema.sql` DDL 配合更简洁，状态流转 SQL 用 `@Modifying @Query` 即可精确表达，无需 MyBatis 的 XML。
- `biz_key` 唯一索引 → 幂等提交的数据库保证（并发冲突时应用层捕获回查，见 §6.1）。
- 状态表只存轻量状态与流转时间，结果大数据（图片 URL 列表等）放结果表，状态查询不被大字段拖慢。
- `idx_task_status` 支撑按状态统计；`idx_task_next_retry` 支撑重试调度扫描；`idx_task_heartbeat` 支撑失联任务回收扫描。
- **结果清理策略**：`task_result` 表只存 `SUCCESS` 的结果；`FAILED/DEAD_LETTER` 的错误信息已落在 `task.fail_reason`，不写结果表，避免无效膨胀。定时任务（如每日）清理 `finished_at` 超过保留天数（默认 30 天，可配置）的 `SUCCESS` 结果行，状态表保留用于统计追溯。清理与状态流转解耦，不影响幂等查询。

---

## 6. 核心模块详细设计

### 6.1 任务提交与幂等

`TaskService.submit(SubmitRequest)`：

1. **幂等检查（TOCTOU 处理）**：按 `bizKey` 查 task 表。
   - 若存在且非终态（`PENDING/QUEUED/PROCESSING/RETRY_WAITING`）→ 返回原 `taskId`，不重复入队。
   - 若存在且为终态（`SUCCESS/FAILED/DEAD_LETTER`）→ 按规则：默认明确拒绝（返回已完成结果或错误码 `TASK_ALREADY_FINALIZED`）；可配置 `allow-resubmit-when-final=true` 时允许同业务键重提（生成新 `taskId`，`biz_key` 加版本后缀避免唯一冲突）。
   - **并发冲突兜底**：查不到时才 `INSERT`，若并发请求同时查不到，其中一个会命中 `uk_task_biz_key` 抛 `DataIntegrityViolationException`。此时**不能直接抛 500**，必须捕获后回查 `bizKey` 并返回已存在的 `taskId`/终态结果——幂等的最终一致性由唯一索引兜底，应用层捕获冲突回查保证用户无感。
2. **持久化（接收成功边界）**：`INSERT task (status=PENDING)`。此步事务提交成功即代表「接收成功」，此后不丢失。
3. **入队（带状态守卫）**：调用 `TaskDispatcher.enqueue(task)`，内部执行条件 UPDATE `PENDING → QUEUED`（见 §6.9），`affected rows == 1` 才算入队成功。这样即使 `QueueRefiller` 回填线程并发操作同一条 `PENDING` 记录，也只有一方能更新成功，杜绝重复入队。
4. **背压处理**：若队列满 `offer` 超时返回 false，按配置的 `BackpressureStrategy` 处理（见 §6.3）——任务已落库为 `PENDING`，不会丢失，由回填线程后续补入队。
5. **返回 taskId**：入队尝试受 `offer-timeout-ms`（默认 50ms）约束，入队成功或超时判定都在此窗口内完成，整体仍为毫秒级，满足「快速返回」；超时则按背压策略返回 429 / `QUEUE_FULL`（`REJECT`）或落库成功（`PERSIST_ONLY`）。**不在落库后立即返回**——那样背压信号（429）就发不出来，与「明确背压策略」冲突。

> 关键边界：先落库、后入队。**落库成功 = 不丢失边界**（此后崩溃可恢复）；入队结果决定本次响应是 200 还是 429——入队是内部动作，即使入队时进程崩溃，`PENDING` 任务会在重启时被回填（见 §6.7），不丢失。入队与回填的并发安全靠「条件 UPDATE 守卫」保证（见 §6.9）。

### 6.2 有界优先级阻塞队列（自行实现）

**不使用 JDK 无界的 `PriorityBlockingQueue`，自行实现 `BoundedPriorityBlockingQueue`**。它**实现 JDK 的 `BlockingQueue<E>` 接口**（直接回应试题「基于 `BlockingQueue`、`Lock`、`Condition` 等自行实现」），但阻塞语义与优先级堆完全基于 `ReentrantLock` + 两个 `Condition`（`notEmpty` / `notFull`）+ 二叉最小堆手写，不继承 JDK 任何队列实现。

```java
public class BoundedPriorityBlockingQueue<E extends PriorityAware>
        implements BlockingQueue<E> {
    private final Object[] heap;          // 数组实现的二叉堆
    private final int capacity;
    private int size;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull  = lock.newCondition();
    // 比较：priority 大者优先（或自定义 Comparator）
}
```

核心方法：

| 方法 | 行为 |
| --- | --- |
| `offer(E, long, TimeUnit)` | 加锁；满则 `notFull.awaitNanos` 超时返回 false（背压触发点）；否则插入堆、`siftUp`、`notEmpty.signal` |
| `put(E)` | 满则阻塞，不超时（供恢复线程用，生产侧不用） |
| `take()` | 空则 `notEmpty.await()`；否则弹出堆顶、`siftDown`、`notFull.signal` |
| `poll(long, TimeUnit)` | 带超时取，供工作线程 |
| `drainTo(Collection)` | 加锁一次性取出全部；**优雅停机时用于「内存队列安全回写」**（见 §6.7），也可供测试与排障查看队列快照 |
| `size()` / `remainingCapacity()` | 加锁读取，供指标 |

优先级：堆顶始终是当前最高优先级任务；同优先级按 `createdAt` 升序（FIFO 兜底），保证公平性。

> 实现要点：所有入队/出队/读方法都在同一把锁内完成，保证堆结构的线程安全；`signal` 只在状态真正变化时发出，避免惊群。

### 6.3 容量治理与背压策略

**容量决策**（静态配置为主，提供自适应选项）：

- 配置项 `queue.capacity` 优先级：**`capacity > 0` 时使用该绝对值**（如示例配置 2000），直接生效；**`capacity = 0` 时才走公式 `corePoolSize × 队列系数`**（系数默认 100，可配置 `queue.coefficient`）。这样示例配置与公式不会冲突——显式配置覆盖默认推导。
- 容量含义：工作线程能并发处理的任务数 × 缓冲倍数；缓冲倍数越大，抗突发能力越强但内存占用与最大延迟越高。
- 自适应（可选，`adaptive=true`）：后台 `CapacityTuner` 每 N 秒采样三个信号——**消费速率**、**队列水位**、**下游处理耗时**（`task.process_time` 的均值/P95）。判定逻辑：当水位长期 > 80% 且（消费速率低于入队速率 **或** 下游耗时持续升高）时扩容（上限 `max-capacity`）；水位长期 < 20% 且下游耗时稳定时缩容（下限 `min-capacity`）。下游耗时升高往往是队列堆积的前兆（单任务处理变慢 → 消费速率下降 → 水位上升），把它纳入信号可提前触发扩容或背压，而非等水位满了才反应。缩容不强制弹出元素，仅影响后续 `offer` 的容量判定（`size > newCapacity` 时不再入队，等消费下降）。

**背压策略**（队列满 `offer(offer-timeout-ms)` 超时返回 false 时触发，可配置，**背压判定发生在 Controller 返回之前**，因此可以向客户端回明确响应）：

1. **`REJECT`（默认）**：快速失败，向客户端返回 `HTTP 429 / 错误码 QUEUE_FULL`，明确告知稍后重试。任务仍已落库为 `PENDING`（不丢失），由回填线程在水位下降时补入队。对客户端最诚实，不把压力转嫁给服务端线程。
2. **`PERSIST_ONLY`**：不落内存队列，仅保持 `PENDING`，完全依赖回填线程异步入队（写库即成功，吞吐受 DB 限制）。适用于「宁可慢、不可拒」的场景。
3. **`BLOCK`**：提交线程阻塞等待空位。**仅允许内部调用（非 HTTP 请求线程）使用**——外部 HTTP 提交禁用，否则 Tomcat 工作线程被阻塞会拖垮网关，与 §7「提交接口快速返回」冲突。

**回填机制（`QueueRefiller`）**：定时（如每 500ms）扫描 `PENDING` 且未入队的任务，按 `remainingCapacity` 批量入队。**入队必须走条件 UPDATE 守卫 `PENDING → QUEUED`（见 §6.9）**：只有 `affected rows == 1` 才真正 `offer` 进内存队列，避免与提交线程的并发入队造成重复。这是背压下「不丢失」的兜底，也是「入队不重」的关键。

### 6.4 线程池与任务处理

> **入队模型明确选择「模型 A：自驱 worker」**（而不是用 `ThreadPoolExecutor.execute()` 把任务投进它的内部队列）。原因：① 试题要求队列自行实现，背压触发点必须由我们控制；② 若走 `execute()`，队列满时只会触发 `RejectedExecutionHandler`，此时 Controller 早已返回 taskId，无法再向客户端回 429，背压时机完全失效。

`TaskExecutor` 的职责拆分：

- 内部持有 `ThreadPoolExecutor`，但**只在启动时 `execute()` 提交 `corePoolSize` 个常驻 worker Runnable**，之后不再调用 `execute()`。
- 每个 worker 的主循环：`while (running) { Task task = queue.take(); 抢执行权; 处理; }`。
- `queue` 是上面实现的 `BoundedPriorityBlockingQueue`——它是**唯一的入队口**，提交侧、回填、重试放行都直接 `queue.offer()`，背压判定（`offer` 返回 false）由此产生，时机在 Controller 返回之前。
- `corePoolSize` = 可配置（默认 `2 × CPU`），`maximumPoolSize = core`（固定大小，不靠扩容抗突发，抗突发靠有界队列缓冲 + 背压）。
- 线程工厂：命名 `task-worker-%d`，设未捕获异常处理器，worker 异常退出后由 `TaskExecutor` 补启新 worker（保持常驻线程数稳定），避免线程静默死亡导致队列无人消费。

`TaskHandler.handle(task)`：
- 用 `sleep` 或本地计算（如循环计算）模拟模型推理耗时（3~30s 随机，可注入），**必须带调用超时上限**（默认 90s），确保 hang 住会被中断而非无限阻塞 worker。
- **异常映射到分类**（统一在 handler 外层判定，见 §6.5）：handler 抛出 `TaskException`（携带 `boolean retryable`）表示业务异常；其他未受检异常按系统异常处理。具体：
  - 抛 `TaskException(retryable=true)` → 可重试业务异常（模型超时、5xx、限流）
  - 抛 `TaskException(retryable=false)` → 不可重试业务异常（内容审核拒绝、参数非法）
  - 抛瞬态系统异常（连接重置等，可白名单） → 可重试系统异常
  - 其他未知异常 / `Error` → 不可恢复
- 处理成功：写结果表 + 改状态 `SUCCESS` 在同一事务（见 §6.5）。
- worker 处理过程中每 `heartbeat-interval`（默认 30s）刷新一次 `task.heartbeat_at = now`，供恢复逻辑精确判断失联（见 §6.7）。

### 6.5 失败分类与重试

**失败分类（异常 → 类别的映射在 worker 调用 handler 的外层统一判定，见 §6.4）：**

| 类别 | 判定（异常信号） | 处理 |
| --- | --- | --- |
| 业务可重试 | `TaskException(retryable=true)`：模型超时、5xx、限流 | 重试，指数退避 |
| 业务不可重试 | `TaskException(retryable=false)`：内容审核拒绝、参数非法 | 直接 `FAILED` |
| 系统可恢复 | 白名单内的瞬态异常（连接重置、SocketTimeoutException 等） | 重试 |
| 系统不可恢复 | 其他未知异常 / `Error` / 序列化失败 | 直接 `FAILED`，记录原因 |

**重试计数与超限判定（统一在失败处理时完成，避免多处并发自增）：**

worker 捕获异常后，在**同一事务内**按以下顺序处理（计数、判定、落库一次完成）：

1. `retryCount++`（先自增，代表「这是第几次尝试失败」）。
2. 写 `fail_reason`（最近一次原因）、`updated_at`。
3. 判定：
   - 不可重试类别 → 状态置 `FAILED`（终态），事务提交。
   - 可重试类别 且 `retryCount < maxRetry` → 计算退避 `backoff = min(maxBackoff, base × 2^(retryCount-1))`，加随机抖动 ±20%，写 `next_retry_at = now + backoff`，状态置 `RETRY_WAITING`，事务提交。**不入内存队列**。
   - 可重试类别 且 `retryCount >= maxRetry` → 状态置 `DEAD_LETTER`（终态），事务提交。不再等待调度器，超限即死信。

**事务边界（关键）**：上述「状态变更 + 计数/原因写入」必须在**一个数据库事务内提交**；成功路径同理——「写 `task_result` 表 + 更新 task 状态为 `SUCCESS`」在同一事务内提交。杜绝「结果已写但状态还是 PROCESSING」的不一致崩溃窗口。

**`RetryScheduler`（职责单一：只放行，不计数不判定）**：
- 独立 `ScheduledExecutorService`（1~2 线程），每秒扫描 `status='RETRY_WAITING' AND next_retry_at <= now` 的任务。
- 对每条执行条件 UPDATE 守卫 `RETRY_WAITING → QUEUED`（见 §6.9）：`affected rows == 1` 才 `offer` 进内存队列；返回 0 说明已被其他线程（如并发调度或提交）处理，跳过。
- 调度器不做 `retry_count` 自增、不做 `maxRetry` 判定——这些在失败处理时已完成，职责单一无并发歧义。

> 「重试不得无限放大队列压力」的保证：① 重试任务失败时不立即入队，而是进退避等待区（DB 中），由调度器按 `nextRetryAt` 限速放行；② `maxRetry` 上限截断到死信；③ 调度器放行走条件 UPDATE 守卫，不会重复入队。

### 6.6 状态与结果查询 / 统计

`TaskService`：

- `getTask(taskId)`：查 task 表 + result 表，返回状态、结果、失败原因、已重试次数。
- `getStats()`：`SELECT status, COUNT(*) FROM task GROUP BY status`，返回积压（`PENDING+QUEUED+RETRY_WAITING`）、处理中（`PROCESSING`）、失败（`FAILED+DEAD_LETTER`）、成功（`SUCCESS`）。

### 6.7 优雅停机与启动恢复

`ShutdownManager` 实现 `SmartLifecycle`（设较高 `phase`，保证在数据源 bean 之前 stop），停机顺序：

1. **停收新任务**：置 `accepting=false`，提交接口直接返回 `SERVICE_SHUTTING_DOWN`（503）。
2. **停止后台调度线程**：先停 `RetryScheduler`、`QueueRefiller`、`CapacityTuner`，避免停机过程中还在入队。
3. **工作线程池收敛**：`executor.shutdown()`（不再接收新任务，让正在处理的跑完）；置 worker 主循环 `running=false` 使其停止 `take`；`awaitTermination(await-termination-ms)` 等待；超时未完成则 `shutdownNow()` 中断 worker。被中断的任务保持 `PROCESSING`，其最后心跳停在中断前，重启时由恢复逻辑按「心跳过期」回收重试。
4. **内存队列安全回写**：在 worker 已停止 `take` 后，执行 `queue.drainTo(list)` 取出未处理任务，对每条做一次持久化确认——这些任务在库中本就是 `QUEUED` 或 `PENDING`（入队前已落库），回写动作将内存态与磁盘态显式对齐（批量确认状态一致，必要时写入停机快照标记），保证重启后可恢复。**此步严格对应试题「把内存队列中未处理的任务安全回写」**。
5. **最后关闭数据源**：必须在 `drainTo` 回写完成、且 `awaitTermination` 完成（或超时）**之后**再关 H2 连接池，避免被中断的 worker 还在写 `task_result`/更新状态时数据源已关闭导致写入失败、任务卡在 `PROCESSING`。`SmartLifecycle` 的 phase 机制保证此顺序。

> 「安全回写」不依赖数据是否已在磁盘——即使入队前已持久化，仍**显式执行 `drainTo` 回写确认**（字面满足试题要求），使停机瞬间内存与磁盘一致；其数据本就在 DB，故回写不引入额外正确性风险。同时确保没有「已标记 PROCESSING 但未落最终状态/结果」的任务被静默丢失——这通过「停机限时等待 + 重启心跳过期回收」兜底。

**启动恢复 `RecoveryService`（在应用就绪后、流量进入前执行一次，之后由心跳巡检周期续跑）：**

- `PENDING` / `QUEUED` → 走条件 UPDATE 守卫重新入内存队列（`affected rows==1` 才 offer）。
- `PROCESSING` 且 **`heartbeat_at` 距今 > `heartbeat-timeout-ms`**（默认 120s）→ 视为失联（worker 崩溃或被强杀），转 `RETRY_WAITING` 并重试。**用心跳而非 `startedAt` 判定**：文生图任务 P99 达 60s，若模型调用 hang 住但心跳还在刷新，说明任务仍在跑，不能误杀重复执行；只有心跳停止超过阈值才认定失联。
- `RETRY_WAITING` 且已到 `nextRetryAt` → 走守卫入队。
- 为防启动与正在运行的 worker 并发（极端情况下重启极快），所有入队/回收都走条件 UPDATE 守卫（见 §6.9），`affected rows==0` 即跳过。

### 6.8 可观测性

统一使用 **Micrometer**（Spring Boot Actuator 自带，不自研计数器/滑窗），在状态流转的关键点埋点，通过 `/actuator/metrics` 暴露，并在自定义 `/api/tasks/metrics` 汇总展示：

| 指标 | 类型 | 来源 / 埋点时机 |
| --- | --- | --- |
| `queue.size` / `queue.capacity` / `queue.remaining` | Gauge | 队列 `size()` / `capacity`（实时读取） |
| `executor.active` / `executor.pool_size` | Gauge | 线程池 `getActiveCount()` / `getPoolSize()` |
| `task.success.total` / `task.failed.total` / `task.dead_letter.total` / `task.retry.total` | Counter | 状态流转为对应终态/重试时 `increment()` |
| `task.queue_time` | Timer | worker 抢到任务时 `record(startedAt - enqueuedAt)` |
| `task.process_time` | Timer | 任务完成时 `record(finishedAt - startedAt)` |
| `task.submit.total` / `task.submit.rejected` | Counter | 提交成功 / 背压拒绝时 |

说明：`Counter` 只增不减，重启后从 0 开始（与 DB 持久统计互补——`getStats()` 走 DB 是全量历史计数，Micrometer 是进程内运行时计数，两者用途不同，文档明确区分避免混淆）。`Timer` 提供均值、P95、P99，比自实现滑窗更准确。

### 6.9 状态流转守卫（不丢不重的核心契约）

整个系统「不丢不重」的根基是：**所有状态变更都用带前置状态条件的 UPDATE，以 `affected rows == 1` 作为「抢到执行权」的唯一判据**。任何 check-then-act（先查状态再写）都有竞态，必须用条件 UPDATE 原子化。编码时以下 SQL 是强制契约，所有模块（提交、回填、重试放行、worker 取任务、恢复）共用：

| 流转 | 调用方 | 条件 UPDATE | 成功含义 |
| --- | --- | --- | --- |
| `PENDING → QUEUED` | 提交 / `QueueRefiller` 回填 / 启动恢复 | `UPDATE task SET status='QUEUED', enqueued_at=now WHERE task_id=? AND status='PENDING'` | 返回 1：本线程抢到，`offer` 入内存队列；返回 0：别人已入队，跳过 |
| `RETRY_WAITING → QUEUED` | `RetryScheduler` / 启动恢复 | `UPDATE task SET status='QUEUED', enqueued_at=now WHERE task_id=? AND status='RETRY_WAITING' AND next_retry_at<=now` | 返回 1 才入队；返回 0 跳过（未到期或已被放行） |
| `QUEUED → PROCESSING` | worker 取到任务后 | `UPDATE task SET status='PROCESSING', started_at=now, heartbeat_at=now WHERE task_id=? AND status='QUEUED'` | **返回 1 才真正执行业务**；返回 0 说明队列中是重复项，直接丢弃不执行 |
| `PROCESSING → SUCCESS` | worker 成功（与写 result 同事务） | `UPDATE task SET status='SUCCESS', finished_at=now WHERE task_id=? AND status='PROCESSING'` | — |
| `PROCESSING → RETRY_WAITING` | worker 失败可重试（同事务） | `UPDATE task SET status='RETRY_WAITING', retry_count=retry_count+1, fail_reason=?, next_retry_at=? WHERE task_id=? AND status='PROCESSING'` | 自增与判定在同一 SQL/事务，避免并发重复计数 |
| `PROCESSING → DEAD_LETTER` | worker 失败超限（同事务） | `UPDATE task SET status='DEAD_LETTER', retry_count=retry_count+1, fail_reason=?, finished_at=now WHERE task_id=? AND status='PROCESSING'` | — |
| `PROCESSING → FAILED` | worker 不可恢复失败（同事务） | `UPDATE task SET status='FAILED', fail_reason=?, finished_at=now WHERE task_id=? AND status='PROCESSING'` | — |
| 心跳刷新 | worker 处理中 | `UPDATE task SET heartbeat_at=now WHERE task_id=? AND status='PROCESSING'` | 不影响状态，仅续命 |
| `PROCESSING → RETRY_WAITING`（失联回收） | `RecoveryService` | `UPDATE task SET status='RETRY_WAITING', fail_reason='heartbeat_timeout', retry_count=retry_count+1, next_retry_at=now WHERE task_id=? AND status='PROCESSING' AND heartbeat_at < ?` | 返回 1 才入队重试，防止与仍在跑的 worker 冲突 |

**为什么这能保证不丢不重：**

- **不丢**：状态以 DB 为准，内存队列只是缓存。任何路径都先有 DB 状态，再操作内存；`offer` 失败不影响 DB 状态（仍是 `PENDING`/`RETRY_WAITING`），由回填/调度兜底。
- **不重**：每条状态转移的「前置状态」是唯一的，DB 行级锁保证只有一个线程的条件 UPDATE 返回 1。内存队列里可能因竞态出现同一 task 的两个引用（例如提交线程与回填线程都 `offer` 了），但 worker 取到后执行 `QUEUED → PROCESSING` 守卫，**只有第一个返回 1 会执行业务，第二个返回 0 直接丢弃**——重复只发生在内存队列的引用层，不发生在执行层。

> 这是面试「怎么保证不丢不重」的标准答案：状态机 + 条件 UPDATE 守卫 + 内存幂等丢弃。没有这一层，任何「先查后写」的实现都会在并发下重复入队或重复执行。

---

## 7. 接口设计（REST API）

| 方法 | 路径 | 说明 | 关键入参/出参 |
| --- | --- | --- | --- |
| POST | `/api/tasks` | 提交任务 | 入：`bizKey`、`taskType`、`priority`、`payload`；出：`taskId`、`status` |
| GET | `/api/tasks/{taskId}` | 查询任务状态与结果 | 出：状态、结果、`failReason`、`retryCount`、各时间戳 |
| GET | `/api/tasks` | 按状态/分页列表 | 入：`status`、`page`、`size` |
| GET | `/api/tasks/stats` | 按状态统计数量 | 出：各状态计数 |
| GET | `/api/tasks/metrics` | 运行时指标 | 出：队列长度/容量、活跃线程、成功/失败/重试计数、平均排队/处理时长 |

提交接口**同步仅做：幂等检查 + 落库 + 尝试入队**，保证快速返回（毫秒级），处理异步进行。

---

## 8. 配置设计（application.yaml）

```yaml
spring:
  application:
    name: async-task-queue
  datasource:
    url: jdbc:h2:file:./data/async-task;DB_CLOSE_ON_EXIT=FALSE;AUTO_RECONNECT=TRUE
    driver-class-name: org.h2.Driver
    username: sa
    password:
  jpa:
    hibernate:
      ddl-auto: none          # 用 schema.sql 显式建表，避免自动建表不可控
    show-sql: false
  sql:
    init:
      mode: always            # 启动执行 schema.sql 建表（IF NOT EXISTS）

async-task:
  submit:
    allow-resubmit-when-final: false  # 终态任务同 bizKey 是否允许重提（false=拒绝）
  queue:
    capacity: 2000            # >0 用该绝对值；=0 时走 corePoolSize × coefficient
    coefficient: 100          # capacity=0 时的推导系数
    min-capacity: 500         # 自适应下限
    max-capacity: 10000       # 自适应上限
    adaptive: false           # 是否自适应调整
    backpressure: REJECT      # REJECT / PERSIST_ONLY / BLOCK（BLOCK 仅限内部调用）
    offer-timeout-ms: 50      # offer 超时，触发背压
  executor:
    core-pool-size: 8         # 默认 2*CPU
    max-pool-size: 8          # 固定大小，抗突发靠队列缓冲+背压
    thread-name-prefix: task-worker
  retry:
    max-retry: 3
    base-backoff-ms: 1000
    max-backoff-ms: 60000
    jitter: true
    scheduler-pool-size: 2
  handler:
    call-timeout-ms: 90000    # 单次任务调用超时上限，hang 住即中断
  heartbeat:
    interval-ms: 30000        # worker 处理中刷新心跳的间隔
    timeout-ms: 120000        # 心跳停止超过该值视为失联（= 任务最大耗时 + 心跳间隔 + 余量）
  cleanup:
    result-retention-days: 30 # SUCCESS 结果保留天数，超期定时清理
  shutdown:
    await-termination-ms: 30000
```

---

## 9. 关键设计取舍（面试三大深挖重点）

> 试题明确：面试会针对「**不丢失的实现边界**」「**容量与背压**」「**重试与幂等**」深挖。本节把这三块的设计取舍、实现细节、追问应答集中整理，作为答辩底稿。这三点的共同根基是 **§6.9 状态流转条件 UPDATE 守卫**——所有正确性保证最终都收敛到它。

### 9.1 「不丢失」的实现边界

**核心论断**：接收成功边界 = `INSERT task(status=PENDING)` 的**事务提交成功**那一刻。此后无论进程何时崩溃，任务都在 H2 文件库中，可被恢复。

**边界两侧的崩溃处理：**

| 崩溃时刻 | DB 状态 | 后果 | 兜底 |
| --- | --- | --- | --- |
| 落库事务提交前 | 无记录 | 客户端收不到成功响应 | 客户端超时重试，`bizKey` 幂等保证不重复 |
| 落库后、入队前（或入队中） | `PENDING` | 内存没入队 | 启动 `RecoveryService` 回填；或 `QueueRefiller` 定时扫到补入队 |
| 入队后、worker 取走前 | `QUEUED` | 内存引用丢失 | 启动回填（库状态仍是 `QUEUED`） |
| worker 处理中崩溃 | `PROCESSING` + 心跳停 | 任务悬停 | 心跳过期 → 恢复逻辑回收重试 |
| 处理完、写结果事务提交前 | `PROCESSING` | 结果未落 | 重启后心跳过期回收，重新执行（幂等 `bizKey` + 结果覆盖，不产生重复业务结果） |

**为什么不丢：状态以 DB 为唯一真相源，内存队列只是加速器。** 「先持久化、后入队；内存可丢、DB 兜底」。停机时仍**显式执行 `drainTo` 安全回写**（见 §6.7）使内存与磁盘对齐——其数据本就在 DB，故回写不引入额外正确性风险，同时字面满足试题「安全回写」要求。

**面试追问 → 应答：**
- 「H2 文件库会不会丢？」→ 文件模式 + `DB_CLOSE_ON_EXIT=FALSE` 正常停机不丢；OS 掉电属于物理故障范畴，单机系统不承诺，生产需换 MySQL/Postgres。
- 「写 result 和改状态分开会怎样？」→ 会出现「结果有了但状态还是 PROCESSING」，所以放同一事务（§6.5 事务边界）。
- 「worker 处理到一半崩溃，任务会不会重复执行？」→ 会重试一次（心跳过期回收），所以**业务执行本身要幂等**（文生图场景同 `bizKey` 重跑覆盖结果），这是「至少一次」语义下的必然要求，文档已声明。

### 9.2 容量与背压

**核心论断**：有界队列不是为了限流，而是为了**在过载时强制系统做出明确决策**，而不是默默堆到 OOM。

**选型依据：**

1. **为什么有界而非无界**：下游（模型推理）变慢时，无界队列会无限堆积任务对象（含 payload JSON），直到 OOM 雪崩；有界队列到顶即触发背压，把「能否接收」的决策显式化。
2. **容量怎么定**：`capacity > 0` 用显式绝对值（覆盖默认推导）；`=0` 走 `corePoolSize × 系数(100)`。含义是「并发处理能力 × 缓冲倍数」——缓冲越大抗突发越强，但内存占用与最大排队延迟越高。自适应（按消费速率/水位动态调）作为可选项，默认关闭，因为静态配置在流量可预测时更简单稳定。
3. **为什么默认 `REJECT`（429）**：对客户端最诚实，明确告知「现在满了请稍后重试」，而不是把请求线程阻塞住拖垮网关（`BLOCK`）或假装成功实际无限延迟（无界队列）。`PERSIST_ONLY` 是「宁可慢不可拒」的折中。
4. **背压时机为什么必须在 Controller 返回前**：这是选「模型 A 自驱 worker」而非 `ThreadPoolExecutor.execute()` 的根因——后者队列满只走拒绝策略，此时 Controller 已返回，背压失效（§6.4）。我们的背压是 `queue.offer()` 返回 false，在 submit 同步链路里，能直接回 429。
5. **背压下怎么不丢**：被拒绝的任务已落库为 `PENDING`，`QueueRefiller` 每 500ms 扫 `PENDING` 按 `remainingCapacity` 补入队（走 §6.9 守卫防重）。过载缓解后自动追上，无需客户端介入。

**面试追问 → 应答：**
- 「队列满了返回 429，客户端一直重试不是放大压力？」→ 429 带 `Retry-After` 头引导客户端退避；服务端 `PENDING` 已存，客户端重试命中幂等返回原 taskId，不会重复创建。
- 「为什么不扩容线程池抗突发？」→ IO 密集任务线程数本就 > CPU，再扩会加剧上下文切换与下游模型并发压力（模型也会限流），所以固定池大小 + 队列缓冲 + 背压更可控。
- 「自适应缩容时队列里还有元素怎么办？」→ 缩容只改容量判定不弹元素，等消费自然下降；不强制 `remove`，避免锁竞争与元素丢失。

### 9.3 重试与幂等

**幂等的实现层次：**

1. **提交幂等**：`bizKey` 唯一索引是最终保证。应用层先查后插存在 TOCTOU（两请求同时查不到同时插），所以**捕获 `DataIntegrityViolationException` 后回查返回已存在 taskId**，而非抛 500（§6.1）。
2. **入队幂等（不重复入队）**：靠 §6.9 的 `PENDING → QUEUED` 条件 UPDATE，提交线程与回填线程并发时只有一方 `affected rows==1`。
3. **执行幂等（不重复执行）**：内存队列可能因竞态存在同一 task 的两个引用，但 worker 取到后先做 `QUEUED → PROCESSING` 守卫，只有第一个返回 1 会执行业务，第二个返回 0 直接丢弃（§6.9）。
4. **业务执行幂等**：系统是「至少一次」语义（崩溃恢复会重跑），所以业务 handler 对同 `bizKey` 的重入必须覆盖而非追加（文生图：同 promptHash 重新生成覆盖旧结果，不产生重复计费）。这是使用方契约，文档显式声明。

**重试为什么不放大队列压力（三条防线）：**

1. 失败时**不立即重新入队**，而是写 `RETRY_WAITING` + `nextRetryAt` 退避，任务待在 DB 里不占内存队列。
2. `RetryScheduler` 按 `nextRetryAt` **限速放行**，每秒扫描到期任务入队，削平重试尖峰。
3. `maxRetry` 上限截断到 `DEAD_LETTER`，超限不再自动重试。
4. 调度器放行也走 §6.9 守卫（`RETRY_WAITING → QUEUED` 带 `next_retry_at<=now` 条件），不会重复入队。

**重试计数为什么在失败处理时自增，而不是调度器入队时自增：** 自增点唯一（worker 失败事务内），调度器只放行不计数，避免「worker 写 RETRY_WAITING」和「调度器入队」两处并发自增导致计数错乱或跳过重试。`retry_count >= maxRetry` 时直接置 `DEAD_LETTER`，不再进调度器等待（§6.5）。

**面试追问 → 应答：**
- 「不用分布式锁怎么保证幂等？」→ 单机进程内，DB 唯一索引 + 条件 UPDATE 行锁已足够，分布式锁是多实例才需要的重武器。
- 「死信了怎么处理？」→ 保留 `fail_reason` 不自动丢，提供人工查询/重放入口（手动把 `DEAD_LETTER` 改回 `PENDING` 重新走流程），支持补偿。
- 「退避时间怎么算？」→ `min(maxBackoff, base × 2^(retryCount-1))` ±20% 抖动，抖动防止大量任务同一时刻到期同时入队造成「重试共振」。

---

## 10. 项目结构

```
src/main/java/com/z/asynctaskqueue/
├── AsyncTaskQueueApplication.java
├── api/
│   ├── TaskController.java
│   └── dto/ (SubmitRequest, TaskResponse, StatsResponse, MetricsResponse)
├── service/
│   ├── TaskService.java            提交/查询/统计
│   ├── TaskDispatcher.java         入队（含条件 UPDATE 守卫调用）
│   ├── RetryScheduler.java         重试放行调度
│   ├── RecoveryService.java        启动恢复 + 心跳失联巡检
│   ├── ShutdownManager.java        优雅停机（SmartLifecycle）
│   └── MetricsService.java         Micrometer 埋点
├── concurrency/
│   ├── BoundedPriorityBlockingQueue.java   ← 核心：自行实现的有界优先级阻塞队列
│   ├── TaskExecutor.java          自驱 worker 线程池（模型 A）
│   ├── TaskHandler.java            任务执行 + 异常分类
│   ├── BackpressureStrategy.java   背压策略枚举
│   ├── QueueRefiller.java          PENDING 回填
│   └── CapacityTuner.java          自适应容量调优（可选）
├── domain/
│   ├── Task.java
│   ├── TaskResult.java
│   ├── TaskStatus.java (enum)
│   └── exception/ (TaskException 携带 retryable 标志 ...)
├── repository/
│   ├── TaskRepository.java         ← 条件 UPDATE 守卫 SQL 集中在此（@Modifying @Query）
│   └── TaskResultRepository.java
└── config/
    ├── AsyncTaskProperties.java    对应 §8 全部配置项
    └── ExecutorConfig.java

src/main/resources/
├── application.yaml
└── schema.sql       ← 建表 DDL（task + task_result）
```

---

## 11. 测试计划（加分项）

- **单元测试**：`BoundedPriorityBlockingQueue` 的线程安全（并发 take/offer 不丢元素、不破坏堆）、优先级顺序、容量边界、`drainTo` 快照完整性；幂等逻辑（含并发冲突回查）；退避时间计算（含抖动边界）。
- **守卫测试（核心）**：针对 §6.9 每条条件 UPDATE，构造「两个线程同时执行同一流转」的并发用例，断言只有一方 `affected rows==1`，另一方为 0；验证同一 task 在内存队列出现两个引用时只执行一次（`QUEUED→PROCESSING` 返回 0 的一方丢弃）。
- **并发测试**：多线程提交 + 消费，高并发下验证不丢不重（用 `bizKey` 计数校验，成功执行次数 == 提交去重数）；队列满时背压生效（429 且任务仍落库，后续被回填追上）。
- **失败重试测试**：模拟可重试/不可重试异常，验证重试次数上限、退避到期才放行、超限直接死信（不再进调度器）；`retry_count` 不自增错乱。
- **恢复测试**：处理中强制停机 → 重启验证心跳过期任务被回收重试、`PENDING/QUEUED` 被回填，无丢失无重复；验证「心跳还在刷新的长任务」不被误杀。
- **集成测试**：`@SpringBootTest` 跑通提交→查询→统计全链路；H2 内存模式用于测试、文件模式用于主程序。

---

## 12. 迭代路线（Git 提交规划）

体现修改过程与时间线（试题要求适时 commit）。**原则：先把「不丢不重」的根基（条件 UPDATE 守卫 + 持久化）打牢，再往上叠功能**，避免后期返工：

1. `init: 项目骨架与依赖`（web / data-jpa / h2 / actuator / validation）
2. `feat(persistence): H2 建表 DDL + 实体 + Repository（含 §6.9 全部条件 UPDATE 守卫方法）` —— 根基先行
3. `feat(core): 自行实现 BoundedPriorityBlockingQueue 及单元测试`
4. `feat(concurrency): TaskExecutor 模型 A（自驱 worker）+ TaskHandler（模拟耗时、调用超时、心跳刷新、异常分类）`
5. `feat(submit): 任务提交与幂等（先落库后入队 + 唯一冲突捕获回查）`
6. `feat(retry): 失败分类、重试调度与退避、死信（计数在失败事务内自增）`
7. `feat(query): 状态/结果查询与按状态统计`
8. `feat(backpressure): 容量治理与背压策略（REJECT / PERSIST_ONLY / QueueRefiller 回填，全部走守卫）`
9. `feat(reliability): 优雅停机（SmartLifecycle 顺序）+ 启动恢复（心跳失联回收）`
10. `feat(metrics): Micrometer 可观测性指标暴露`
11. `docs: 补充 SDD 与容量/背压/不丢失取舍说明（§9 面试底稿）`
12. `test: 守卫并发测试 + 不丢不重验证 + 恢复测试`

---

## 13. 验收清单（对照试题）

| 试题要求 | 设计落点 | 状态 |
| --- | --- | --- |
| 场景建模 + 完整链路 | §3 场景模型、§4.2 链路时序 | ✅ |
| 入队不丢失 | §6.1 先落库后入队、§6.7 恢复、§9.1 边界 | ✅ |
| 有界队列 + 可配置容量 + 背压 | §6.2 自实现有界队列、§6.3 容量与背压 | ✅ |
| 不引入中间件，自行实现 | §6.2 基于 Lock/Condition 实现 | ✅ |
| 任务支持优先级 | §6.2 二叉堆按优先级出队 | ✅ |
| 快速返回 + 持久化边界 + 幂等 | §6.1 | ✅ |
| 失败分类 + 重试上限退避 + 死信 | §6.5 | ✅ |
| 状态/结果查询 + 按状态统计 | §6.6、§7 | ✅ |
| 优雅停机（停收/内存安全回写/限时收敛/可恢复） | §6.7（drainTo 安全回写 + 持久化 + 心跳回收） | ✅ |
| 可观测性指标 | §6.8 Micrometer | ✅ |
| Java 17+ / Spring | Java 25 / Spring Boot 4.2 | ✅ |
| H2 文件持久化 + 数据访问（显式选 Spring Data JPA，不引入 MyBatis）+ DDL 随项目 + 状态/结果表分离 | §5 schema.sql、§5 选型声明、两表分离 | ✅ |
| 模拟耗时，不实接外部服务 | §6.4 sleep/本地计算 + 调用超时 | ✅ |
| 适时 git commit | §12 迭代路线（守卫根基先行） | ✅ |
| 加分项：分层/文档/测试/部署 | §10 分层、本文档、§11 测试 | 部分，随实现补齐 |

**设计正确性加固（review 修复项，面试深挖点）：**

| 加固项 | 设计落点 | 状态 |
| --- | --- | --- |
| 不丢不重的核心契约：状态流转条件 UPDATE 守卫 | §6.9（全表 SQL 契约） | ✅ |
| 入队模型钉死（自驱 worker，背压在 Controller 返回前） | §6.4 模型 A | ✅ |
| 幂等 TOCTOU：唯一冲突捕获回查，不抛 500 | §6.1 | ✅ |
| 失联精确判定：心跳替代 startedAt，不误杀长任务 | §6.7、§3.2、§5.1 | ✅ |
| 事务边界：写结果 + 改状态同一事务 | §6.5 | ✅ |
| 重试计数点唯一（失败事务内自增，调度器只放行） | §6.5 | ✅ |
| 优雅停机：显式 drainTo 安全回写 + 数据源最后关闭 | §6.7 | ✅ |
| 容量配置与公式一致性（>0 覆盖，=0 推导） | §6.3、§8 | ✅ |
| 三大面试重点集中答辩底稿 | §9.1 / §9.2 / §9.3 | ✅ |

**交付/提交要求核对（试题五）：**

| 试题提交要求 | 落实方式 | 状态 |
| --- | --- | --- |
| 可直接启动运行的完整项目 | `mvn spring-boot:run` 或 `java -jar`；H2 文件库自动建表 | 随实现完成 |
| 和 AI 的关键对话放进项目（截图或复制文本） | 归档至 `docs/ai-conversations/`（Markdown 或截图），与 SDD 一并提交 | 待归档 |
| 带 Git 提交记录，能看到迭代修改过程 | 按 §12 迭代路线适时 commit，commit message 带 `feat/fix/docs/test` 前缀 | 随实现完成 |
| Git 仓库地址，或项目压缩包（不含编译文件和日志） | `.gitignore` 排除 `target/`、`*.log`、`data/`（H2 运行时文件库）；提供仓库地址 | 待提交时确认 |