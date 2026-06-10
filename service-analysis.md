# Media Prompt 生成微服务代码分析报告

## 一、异步处理链路分析（从 RabbitMQ 到 MySQL）

### 1.1 完整链路时序图

```mermaid
sequenceDiagram
    participant MQ as RabbitMQ Queue
    participant CONS as MqConsumer
    participant PROC as TaskProcessor
    participant MONGO as MongoDB
    participant SCENE as scene::process_scene
    participant SB as storyboard::process_storyboard
    participant LLM as LlmClient
    participant MYSQL as MySQL
    participant PROD as MqProducer

    Note over MQ,PROD: 消息消费阶段
    MQ->>CONS: delivery (TaskMessage)
    Note right of CONS: await consumer.next()<br>consumer.rs#L173
    CONS->>CONS: 反序列化 JSON → TaskMessage
    alt 反序列化失败
        CONS->>MQ: basic_nack(requeue=false)
        Note right of CONS: 反序列化错误直接丢弃<br>consumer.rs#L183-L186
    end
    CONS->>CONS: 申请 task_semaphore 许可
    Note right of CONS: await acquire_owned()<br>并发控制: max_tasks=5

    Note over MQ,PROD: 任务处理阶段
    CONS->>+PROC: process(task_msg).await
    Note right of PROC: processor.rs#L41

    PROC->>+MONGO: find_script_by_task_id(task_id).await
    Note right of PROC: await 点 #1
    MONGO-->>-PROC: ScriptDocument
    alt 脚本不存在或无场景
        PROC-->>CONS: Err(AppError::TaskFailed)
    end

    loop 每个 scene 串行处理
        PROC->>+SCENE: process_scene(&llm_client, task_id, scene).await
        Note right of SCENE: scene.rs#L25

        Note over SCENE,LLM: Stage 1: video_prompt
        SCENE->>+LLM: generate(vp_ctx).await
        Note right of LLM: await 点 #2<br>llm/client.rs#L51
        LLM->>LLM: 申请 llm_semaphore 许可
        LLM->>LLM: do_request (含重试循环)
        alt LLM 调用成功
            LLM-->>-SCENE: LlmResult
        else LLM 调用失败(场景级)
            LLM-->>-SCENE: Err(AppError::Llm)
            SCENE-->>PROC: Err(传播)
            PROC-->>CONS: Err(任务失败)
        end

        Note over SCENE,LLM: Stage 2: multi_view_prompt
        SCENE->>+LLM: generate(mvp_ctx).await
        Note right of LLM: await 点 #3
        LLM-->>-SCENE: LlmResult

        Note over SCENE,LLM: Stage 3: 分镜级提示词
        loop 每个 storyboard 串行
            SCENE->>+SB: process_storyboard(...).await
            Note right of SB: storyboard.rs#L22

            loop 按 storyboard_sequence 顺序
                SB->>+LLM: generate(ctx).await
                Note right of LLM: await 点 #4..#n
                alt 成功
                    LLM-->>-SB: LlmResult
                    SB->>SB: previous_prompts.push(...)
                else 失败
                    LLM-->>-SB: Err
                    SB->>SB: 记录失败状态 + break
                    Note right of SB: 短路: 后续类型跳过
                    break
                end
            end
            SB-->>-SCENE: StoryboardResult
        end
        SCENE-->>-PROC: SceneResult
    end

    Note over MQ,PROD: 数据持久化阶段
    PROC->>+MYSQL: batch_insert_prompts(&all_prompts).await
    Note right of MYSQL: await 点 #n+1<br>mysql.rs#L31
    MYSQL->>MYSQL: BEGIN 事务
    loop 每 100 条一批 INSERT
        MYSQL->>MYSQL: 执行 chunk INSERT
    end
    MYSQL->>MYSQL: COMMIT
    alt MySQL 写入失败
        MYSQL-->>-PROC: Err(sqlx::Error)
        PROC-->>CONS: Err(任务失败)
    else 写入成功
        MYSQL-->>-PROC: Ok(())
    end

    Note over MQ,PROD: 下游通知阶段
    PROC->>+PROD: publish_completion(task_id).await
    Note right of PROD: await 点 #n+2
    PROD->>MQ: 发布到 media_task_out
    PROD-->>-PROC: Ok(())

    PROC-->>-CONS: Ok(())

    Note over MQ,PROD: 消息确认阶段
    alt 任务全部成功
        CONS->>MQ: basic_ack(delivery_tag)
        Note right of CONS: 处理完再 ACK<br>consumer.rs#L217
    else 任务失败
        CONS->>MQ: basic_nack(requeue=false)
        Note right of CONS: 失败直接丢弃, 无死信<br>consumer.rs#L223-L226
    end
```

### 1.2 关键 async 函数与 await 点汇总

| 序号 | 位置 | 函数 | await 用途 | 错误处理 |
|-----|------|------|-----------|---------|
| 1 | [consumer.rs#L173](file:///Users/huwenjie/项目/gsb/label-02784/backend/src/mq/consumer.rs#L173-L173) | `consumer.next()` | 等待 RabbitMQ 消息投递 | 连接中断 → 触发重连 |
| 2 | [processor.rs#L46](file:///Users/huwenjie/项目/gsb/label-02784/backend/src/task/processor.rs#L46-L46) | `mongo.find_script_by_task_id()` | 从 MongoDB 拉取脚本 | 失败 → 任务失败 → NACK |
| 3 | [scene.rs#L48](file:///Users/huwenjie/项目/gsb/label-02784/backend/src/task/scene.rs#L48-L48) | `llm_client.generate()` (video_prompt) | 生成视频提示词 | 失败 → 任务级失败 → NACK |
| 4 | [scene.rs#L82](file:///Users/huwenjie/项目/gsb/label-02784/backend/src/task/scene.rs#L82-L82) | `llm_client.generate()` (multi_view_prompt) | 生成多视角提示词 | 失败 → 任务级失败 → NACK |
| 5 | [storyboard.rs#L52](file:///Users/huwenjie/项目/gsb/label-02784/backend/src/task/storyboard.rs#L52-L52) | `llm_client.generate()` (storyboard) | 生成分镜提示词 | 单类型失败 → 短路跳过后续 → 记录失败 |
| 6 | [mysql.rs#L31](file:///Users/huwenjie/项目/gsb/label-02784/backend/src/db/mysql.rs#L31-L31) | `batch_insert_prompts()` | 批量写入 MySQL | 失败 → 任务失败 → NACK |
| 7 | [processor.rs#L88](file:///Users/huwenjie/项目/gsb/label-02784/backend/src/task/processor.rs#L88-L88) | `producer.publish_completion()` | 发布完成消息 | 失败 → 任务失败 → NACK |

### 1.3 错误处理策略总结

| 错误类型 | 重试策略 | 跳过策略 | 死信队列 |
|---------|---------|---------|---------|
| 消息反序列化失败 | 无 | 直接 NACK 丢弃 | ❌ 无 |
| MongoDB 查询失败 | 无 | 任务失败 → NACK 丢弃 | ❌ 无 |
| 场景级 LLM 失败 (video_prompt / multi_view_prompt) | LLM 内部重试 3 次 (指数退避) | 任务整体失败 → NACK 丢弃 | ❌ 无 |
| 分镜级 LLM 失败 | LLM 内部重试 3 次 | 当前分镜后续类型短路跳过, 其他分镜继续 | ❌ 无 |
| MySQL 批量写入失败 | 无 | 任务失败 → NACK 丢弃 | ❌ 无 |
| 下游发布失败 | 无 | 任务失败 → NACK 丢弃 | ❌ 无 |
| RabbitMQ 连接断开 | 指数退避重连 (1s~30s) | - | - |

---

## 二、多类提示词的依赖顺序实现分析

### 2.1 依赖结构：分层串行管道

系统采用**分层串行管道 (Layered Sequential Pipeline)** 设计，分为三个层级：

```
任务级 (Task Level)
    ↓ 串行
场景级 (Scene Level): video_prompt → multi_view_prompt
    ↓ 串行 (每个场景依次处理)
分镜级 (Storyboard Level): TextToImage → SpatialCompositionDrawing → FusionImage → SoundEffect → SpecialEffect
```

### 2.2 各层级实现细节

#### 场景级依赖（串行强依赖）

位置: [scene.rs#L34-L100](file:///Users/huwenjie/项目/gsb/label-02784/backend/src/task/scene.rs#L34-L100)

```
video_prompt → multi_view_prompt
```

- **实现方式**: 顺序调用两个 `llm_client.generate().await`，后者的 `PromptContext` 包含前者的输出
- **依赖关系**: `multi_view_prompt` 依赖 `video_prompt` 的结果作为输入上下文
- **失败影响**: 任一失败 → 整个场景失败 → 整个任务失败 → NACK 消息

关键代码证据:
```rust
// video_prompt 结果存入 video_prompt_content
let video_prompt_content = vp_result.content.clone();

// multi_view_prompt 的 ctx 中包含 video_prompt
let mvp_ctx = PromptContext {
    video_prompt: Some(video_prompt_content.clone()),
    ...
};
```

#### 分镜级依赖（串行短路 + 上下文累加）

位置: [storyboard.rs#L38-L115](file:///Users/huwenjie/项目/gsb/label-02784/backend/src/task/storyboard.rs#L38-L115)

```
TextToImage → SpatialCompositionDrawing → FusionImage → SoundEffect → SpecialEffect
```

- **实现方式**: 遍历 `PromptType::storyboard_sequence()` 数组，依次生成
- **依赖关系**: 
  - 所有分镜级类型都依赖 `video_prompt` 和 `multi_view_prompt`（场景级输出）
  - 每个后续类型通过 `previous_prompts` 获得前面所有类型的输出
- **失败影响**: 当前类型失败 → **短路 (break)** → 后续类型全部跳过 → 记录失败状态 → 不影响其他分镜

关键代码证据:
```rust
// 按顺序遍历
for &prompt_type in sequence {
    let ctx = PromptContext {
        previous_prompts: previous_prompts.clone(), // 上下文累加
        ...
    };
    match llm_client.generate(&ctx).await {
        Ok(result) => {
            previous_prompts.push((prompt_type, result.content)); // 累加
        }
        Err(e) => {
            has_error = true;
            break; // 短路: 后续类型不再生成
        }
    }
}
```

#### 场景间依赖（无依赖，串行执行）

位置: [processor.rs#L64-L69](file:///Users/huwenjie/项目/gsb/label-02784/backend/src/task/processor.rs#L64-L69)

- **实现方式**: `for s in &script.scenes` 顺序循环
- **依赖关系**: 场景之间**没有数据依赖**，仅为串行执行
- **失败影响**: 单个场景失败 → 整个任务失败 → 后续场景不再处理

### 2.3 失败影响矩阵

| 失败位置 | 对当前场景的影响 | 对其他场景的影响 | 对整个任务的影响 |
|---------|----------------|----------------|----------------|
| video_prompt 失败 | 场景完全失败 | 后续场景不再处理 | ❌ 任务整体失败 |
| multi_view_prompt 失败 | 场景完全失败 | 后续场景不再处理 | ❌ 任务整体失败 |
| 分镜第 N 个类型失败 | 该分镜后续类型跳过 | 不影响其他分镜 | ✅ 任务继续（记录失败） |
| 某场景的分镜全失败 | 该场景无成功提示词 | 不影响其他场景 | ✅ 任务继续 |

### 2.4 设计优缺点分析

#### 优点

1. **简单直观**: 串行管道模型易于理解和调试，数据流清晰
2. **上下文累加**: `previous_prompts` 机制使后续提示词能参考前面的生成结果，保持风格一致性
3. **分级容错**: 场景级失败终止任务（核心依赖），分镜级失败仅影响当前分镜（部分可用）
4. **失败记录完整**: 即使失败也会将失败状态写入 MySQL，包含错误码、重试次数等元数据

#### 缺点

1. **吞吐量低**: 完全串行执行，无法利用 LLM API 的并发能力。一个任务可能有多个场景和多个分镜，全部串行会导致任务处理时间很长
2. **短路策略过于保守**: 分镜级某一类型失败后，后续类型全部跳过。但实际上后续类型可能并不强依赖前面的每一个类型（例如 `SoundEffect` 可能不需要 `FusionImage` 的结果）
3. **无 DAG 表达能力**: 固定的线性顺序无法表达更复杂的依赖关系（如某些类型可以并行生成）
4. **场景级失败代价高**: 单个场景的 `video_prompt` 失败导致整个任务失败，但可能只是该场景数据有问题，其他场景本可以正常处理
5. **资源浪费**: 场景之间无依赖但串行执行，如果某个 LLM 请求很慢，会阻塞整个任务进度

---

## 三、系统可靠性风险分析

### 3.1 RabbitMQ 消息确认时机分析

#### 当前实现

位置: [consumer.rs#L210-L230](file:///Users/huwenjie/项目/gsb/label-02784/backend/src/mq/consumer.rs#L210-L230)

```rust
join_set.spawn(async move {
    match processor.process(task_msg).await {
        Ok(()) => {
            // 处理成功后 ACK
            ch.basic_ack(tag, BasicAckOptions::default()).await
        }
        Err(e) => {
            // 处理失败后 NACK (不重入队列)
            ch.basic_nack(tag, BasicNackOptions { requeue: false, ..Default::default() }).await
        }
    }
});
```

#### 确认时机判断

- **ACK 时机**: ✅ **处理完再 ACK**（正确模式）
- **NACK 策略**: ❌ `requeue=false` 且无死信队列

#### 风险分析

**风险 1: 失败消息直接丢失**

- **根因**: 任务失败时调用 `basic_nack` 且 `requeue=false`，消息被直接丢弃，没有死信队列 (DLQ) 兜底
- **影响**: 
  - 临时性故障（如 MySQL 短暂不可用、LLM 限流）导致的失败会永久丢失消息
  - 无法进行后续的人工干预或重试分析
  - 数据不一致：部分提示词可能已生成但未写入，任务状态不明确
- **严重程度**: 高

**风险 2: 进程崩溃导致消息重复处理**

- **根因**: 使用 `no_ack=false` 手动确认模式，但如果进程在处理完成前崩溃（如 OOM、强制 kill），未 ACK 的消息会被 RabbitMQ 重新投递给其他消费者
- **影响**:
  - MySQL 写入可能已部分完成（但使用了事务，理论上要么全有要么全无）
  - 下游队列可能已发布完成消息（如果在 ACK 前发布）
  - 重复生成提示词会浪费 LLM token
- **严重程度**: 中（至少消息不会丢，但可能重复处理）

**风险 3: 反序列化失败消息直接丢弃**

- **根因**: [consumer.rs#L181-L186](file:///Users/huwenjie/项目/gsb/label-02784/backend/src/mq/consumer.rs#L181-L186) 反序列化失败直接 NACK 丢弃
- **影响**: 格式错误的消息无法排查，可能是上游 bug 导致
- **严重程度**: 中

#### 改进方案

1. **引入死信队列 (DLQ)**
   - 声明死信交换器和死信队列
   - 业务队列配置 `x-dead-letter-exchange` 指向死信交换器
   - 失败消息 NACK 时 `requeue=false`，自动进入死信队列
   - 后续可通过死信队列进行人工排查或延迟重试

2. **区分可重试错误和不可重试错误**
   - 对于临时性错误（网络抖动、MySQL 暂时不可用），使用 `requeue=true` 或延迟重试
   - 对于永久性错误（数据格式错误、业务逻辑错误），直接进死信队列

3. **幂等性保障**
   - 为 MySQL 写入增加唯一键约束（如 `task_id + scene_index + storyboard_index + prompt_type`）
   - 使用 `INSERT ... ON DUPLICATE KEY UPDATE` 保证重复处理不会产生重复数据

4. **反序列化失败也入死信**
   - 将原始 payload 存入死信队列，保留排查线索

---

### 3.2 LLM API 调用超时和重试策略分析

#### 当前实现

位置: [llm/client.rs#L51-L163](file:///Users/huwenjie/项目/gsb/label-02784/backend/src/llm/client.rs#L51-L163)

```
超时时间: 120 秒 (config.timeout_secs)
最大重试: 3 次 (config.max_retries)
重试间隔: 指数退避 (1s, 2s, 4s)
重试条件: 5xx / 网络错误 → 重试; 4xx → 不重试
并发控制: llm_semaphore (max_llm_requests=10)
```

#### 风险分析

**风险 1: 超时时间过长可能导致任务堆积**

- **根因**: 默认 120 秒超时，如果 LLM 服务响应慢，单个请求会占用连接很长时间
- **影响**:
  - `llm_semaphore` 许可被长时间占用，其他请求排队
  - 任务整体处理时间不可控地延长
  - 可能触发 RabbitMQ 的超时或消费者断开
- **严重程度**: 中

**风险 2: 重试次数和退避策略未考虑速率限制**

- **根因**: 对于 429 (Too Many Requests) 状态码，当前代码归为 4xx 不重试
- **影响**:
  - 遇到限流时直接失败，浪费了一次可以通过等待解决的机会
  - 没有根据 `Retry-After` 响应头动态调整等待时间
- **严重程度**: 中高（生产环境中限流很常见）

**风险 3: 重试风暴风险**

- **根因**: 指数退避起始于 1 秒，且没有引入随机抖动 (jitter)
- **影响**: 多个请求同时失败并同时重试，可能加剧 LLM 服务端压力
- **严重程度**: 低（并发量不大时影响有限）

**风险 4: 请求级超时 vs 总超时不明确**

- **根因**: HTTP client 的 timeout 是单次请求的超时，但重试会叠加总时间
- **影响**: 3 次重试 × 120s 超时 = 最多可能 360s+ 等待时间，远超预期
- **严重程度**: 中

#### 改进方案

1. **区分 429 状态码并特殊处理**
   ```rust
   } else if status == 429 {
       // 从 Retry-After 头获取建议等待时间
       let retry_after = response.headers()
           .get("retry-after")
           .and_then(|v| v.to_str().ok())
           .and_then(|s| s.parse::<u64>().ok())
           .unwrap_or(5);
       Err(LlmError::retryable_with_meta(...)) // 标记为可重试
   } else if status >= 400 && status < 500 {
       // 其他 4xx 不重试
   }
   ```

2. **增加总超时限制**
   - 在 `generate` 函数级别增加整体超时（如使用 `tokio::time::timeout`）
   - 避免多次重试累积过长时间

3. **增加退避抖动**
   ```rust
   let base_delay = Duration::from_millis(1000 * 2u64.pow(attempt - 1));
   let jitter = Duration::from_millis(rand::random::<u64>() % 1000);
   let backoff = base_delay + jitter;
   ```

4. **可配置的超时和重试策略**
   - 不同 prompt type 可以有不同的超时配置（如 video_prompt 可能需要更长）
   - 支持按优先级调整重试次数

5. **熔断机制**
   - 当失败率超过阈值时，暂时停止向该 LLM 端点发送请求
   - 避免在服务端故障时持续发送请求加剧问题

---

### 3.3 MySQL 写入失败时的消息丢失/重复处理分析

#### 当前实现

位置: [mysql.rs#L31-L92](file:///Users/huwenjie/项目/gsb/label-02784/backend/src/db/mysql.rs#L31-L92)

```
写入方式: 批量 INSERT (每 100 条一批)
事务: 单事务包裹所有批次
失败处理: 事务回滚 → 返回错误 → 任务失败 → NACK (requeue=false)
```

#### 风险分析

**风险 1: 写入失败导致消息丢失**

- **根因**: MySQL 写入失败 → 任务返回 `Err` → consumer NACK 且 `requeue=false` → 消息永久丢失
- **影响**:
  - 所有已生成的提示词（可能已经花了很多 LLM token）全部白费
  - 任务状态不明确，上游不知道任务是成功还是失败
  - 数据不一致：MongoDB 中有脚本，但 MySQL 中没有提示词
- **严重程度**: 高

**风险 2: 无幂等性保障，重复处理会产生重复数据**

- **根因**: 表结构中没有针对 `(task_id, scene_index, storyboard_index, prompt_type)` 的唯一约束
- **影响**:
  - 如果消息被重新投递（如消费者崩溃重启），会产生重复记录
  - 下游系统可能基于重复数据产生错误结果
- **严重程度**: 中高

**风险 3: 单事务过大可能影响性能和稳定性**

- **根因**: 所有批次（可能几百上千条记录）在一个事务中
- **影响**:
  - 长事务占用连接时间长，影响并发性能
  - 大事务可能导致 binlog 膨胀或锁等待
- **严重程度**: 低（数据量不大时可接受）

**风险 4: 部分成功 vs 全部成功的取舍**

- **根因**: 单事务保证要么全写成功要么全失败
- **影响**:
  - 如果某个批次因为数据问题失败（如某列超长），所有数据都写不进去
  - 但从数据一致性角度看，全有或全无比部分成功更可预测
- **严重程度**: 低（当前设计是合理的权衡）

#### 改进方案

1. **MySQL 写入失败时消息不直接丢弃**
   - 对于临时性错误（连接断开、死锁等），应该重试写入或让消息重入队列
   - 对于永久性错误（数据格式问题），进入死信队列
   - 建议区分 `AppError::MySql` 的子类型，分别处理

2. **增加唯一约束实现幂等**
   ```sql
   ALTER TABLE tb_media_prompt 
   ADD UNIQUE KEY uk_task_scene_storyboard_type 
   (task_id, scene_index, storyboard_index, prompt_type);
   ```
   - 使用 `INSERT ... ON DUPLICATE KEY UPDATE` 或先查后插
   - 确保重复处理不会产生重复数据

3. **MySQL 写入增加重试**
   - 对于临时性错误（如连接超时、死锁），内部重试 2-3 次
   - 重试间隔可采用指数退避
   - 减少因短暂故障导致的整体任务失败

4. **失败时的部分数据保留策略（可选）**
   - 如果业务允许部分成功，可以考虑逐场景提交
   - 但需要配合状态追踪，记录已写入的进度
   - 权衡：一致性 vs 部分可用性

5. **下游发布与 MySQL 写入的原子性问题**
   - 当前流程：MySQL 写入成功 → 发布下游消息
   - 如果 MySQL 成功但发布失败，任务标记为失败（NACK），但数据已写入
   - 改进：可以考虑在 MySQL 中记录发布状态，或使用事务消息模式

---

## 四、总结与核心改进建议优先级

| 优先级 | 改进项 | 风险类型 | 预期收益 |
|-------|--------|---------|---------|
| P0 | 引入死信队列 | 消息丢失 | 防止失败消息永久丢失 |
| P0 | MySQL 唯一约束 + 幂等写入 | 重复处理 | 保证数据一致性 |
| P1 | 区分可重试/不可重试错误 | 消息丢失 | 临时性故障可自动恢复 |
| P1 | 429 限流重试支持 | LLM 可靠性 | 提高 LLM 调用成功率 |
| P1 | MySQL 写入内部重试 | 消息丢失 | 减少短暂故障导致的失败 |
| P2 | 退避抖动 + 总超时控制 | LLM 可靠性 | 更稳定的重试行为 |
| P2 | 分镜级并行生成 | 性能 | 提高吞吐量 |
| P2 | 熔断机制 | LLM 可靠性 | 故障时的自我保护 |
