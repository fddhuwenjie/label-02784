# Media Prompt 生成微服务

基于 Rust 异步架构的 LLM 提示词生成微服务。监听 RabbitMQ 队列接收任务，从 MongoDB 查询剧本数据，按依赖顺序调用 LLM API 生成 6 类提示词，结果写入 MySQL。

> **跨平台说明**：后端 Dockerfile 基础镜像（`rust:1.78-bookworm` / `debian:bookworm-slim`）原生支持 ARM64 和 x86_64。`mysql:5.7` 与 `mongo:4.0.13` 仅提供 amd64 镜像，docker-compose.yml 中已声明 `platform: linux/amd64`，在 Apple Silicon 上通过 Rosetta 2 运行。

## How to Run

### 环境要求

- Docker >= 20.10
- Docker Compose >= 2.0

### 启动步骤

```bash
# 1. 克隆项目
git clone <repo-url> && cd label-02784

# 2. 配置环境变量（可选）
export LLM_API_KEY="your-api-key-here"

# 3. 一键启动所有服务
docker-compose up --build -d

# 4. 查看日志
docker-compose logs -f backend
```

### 运行模式提示（避免误解）

- `docker-compose` 默认会加载 `backend/config/config.demo.yaml`（`APP_CONFIG_PATH=/app/config/config.demo.yaml`）。
- 默认配置使用内置本地 Mock LLM 端点 `POST /mock-llm`，用于 demo/E2E 验证，不依赖真实外部 LLM。
- 只有切换到 `backend/config/config.yaml` 并提供有效 `LLM_API_KEY` 时，才会调用真实生产 LLM API。

### 切换到生产配置（真实 LLM）

#### Docker Compose 方式

```bash
# 1) 覆盖默认 demo 配置
export APP_CONFIG_PATH=/app/config/config.yaml

# 2) 提供真实 LLM API Key（必填）
export LLM_API_KEY="<your-real-llm-api-key>"

# 3) 重建并启动
docker-compose up --build -d

# 4) 验证后端容器配置路径
docker exec -it media-prompt-backend printenv APP_CONFIG_PATH
```

#### 本地运行方式（不走 docker backend）

```bash
cd backend
APP_CONFIG_PATH=config/config.yaml LLM_API_KEY="<your-real-llm-api-key>" cargo run --release
```

### 一键脚本（启动 + 全功能测试）

仓库提供一键脚本，可自动完成：
- 服务启动与依赖就绪等待
- `/healthz`、`/readyz`、`/metrics` 检查
- Mongo 写入示例剧本、RabbitMQ 投递任务
- MySQL 入库校验、下游队列消息校验
- 非法消息恢复验证
- （默认）RabbitMQ 重连与 MySQL 降级恢复验证

```bash
# 全量测试（推荐）
bash scripts/e2e_full_test.sh

# 仅核心链路（不做重连/降级测试）
bash scripts/e2e_full_test.sh --core-only

# 测试后自动停止容器
bash scripts/e2e_full_test.sh --down
```

可选参数：
- `--task-id <num>`：指定测试任务 ID 基数（脚本会使用 base/base+1/base+2）
- `--health-port <n>`：健康检查端口（默认 `9999`）
- `--no-build`：启动时不执行 `--build`

常见问题：
- 若提示 `Cannot connect to the Docker daemon ... docker.sock`：
  1. 启动 Docker Desktop（确认 Engine running）
  2. 或使用 Colima：`colima start`
  3. 自检：`docker info`
  4. 脚本在 macOS 下会自动尝试拉起 Docker Desktop；若仍失败，请先手动打开 Docker Desktop 后重试

### 端到端验证（示例数据 + 示例消息）

`TaskMessage` 消息结构：

```json
{"task_id": 1234}
```

注意：
- `docker-compose` 默认使用 demo 配置：内置本地 mock LLM（`/mock-llm`），无需有效 `LLM_API_KEY` 即可跑通流程。
- 生产/真实 LLM 场景必须切换到 `backend/config/config.yaml`（`llm.allow_empty_api_key=false`）并提供有效 Key。

启动服务后，按以下步骤完成一次可验证的端到端流程：

```bash
# 0. （仅旧环境升级时需要）补充失败观测字段
# 下列命令如果返回 "Duplicate column name" 可忽略
docker exec -i media-prompt-mysql mysql -uroot -p123456 -D media_db -e "
ALTER TABLE tb_media_prompt
ADD COLUMN llm_retries INT DEFAULT NULL COMMENT 'LLM调用重试次数';"
docker exec -i media-prompt-mysql mysql -uroot -p123456 -D media_db -e "
ALTER TABLE tb_media_prompt
ADD COLUMN llm_error_code VARCHAR(64) DEFAULT NULL COMMENT 'LLM失败错误码';"
docker exec -i media-prompt-mysql mysql -uroot -p123456 -D media_db -e "
ALTER TABLE tb_media_prompt
ADD COLUMN llm_response_snippet TEXT DEFAULT NULL COMMENT 'LLM失败响应片段（截断）';"

# 1. 确保输入/输出队列存在
docker exec -i media-prompt-rabbitmq rabbitmqadmin -u guest -p guest \
  declare queue name=media_task_in durable=true
docker exec -i media-prompt-rabbitmq rabbitmqadmin -u guest -p guest \
  declare queue name=media_task_out durable=true

# 2. 向 MongoDB 写入 task_id=1234 的示例剧本文档
docker exec -i media-prompt-mongodb mongo script_db <<'EOF'
db.scripts.deleteMany({ task_id: NumberLong(1234) });
db.scripts.insertOne({
  task_id: NumberLong(1234),
  title: "README E2E 示例",
  scenes: [
    {
      scene_index: 1,
      description: "雨夜街头，霓虹灯反射在地面",
      storyboards: [
        { storyboard_index: 1, content: "远景，主角走入画面中央" },
        { storyboard_index: 2, content: "近景，主角抬头看向招牌" }
      ]
    }
  ]
});
EOF

# 3. 发送任务消息到消费队列
docker exec -i media-prompt-rabbitmq rabbitmqadmin -u guest -p guest \
  publish routing_key=media_task_in payload='{"task_id":1234}'

# 4. 观察后端处理日志（成功后会 ACK 并投递到 media_task_out）
docker-compose logs -f backend
```

处理完成后可使用以下命令验证结果：

```bash
# 5. 查看 MySQL 入库结果
docker exec -i media-prompt-mysql mysql -uroot -p123456 -D media_db -e "
SELECT task_id, scene_index, storyboard_index, prompt_type, status,
       llm_retries, llm_error_code, llm_response_snippet, error_message
FROM tb_media_prompt
WHERE task_id = 1234
ORDER BY id;"

# 6. 查看下游队列消息
docker exec -i media-prompt-rabbitmq rabbitmqadmin -u guest -p guest \
  get queue=media_task_out ackmode=ack_requeue_false count=10
```

### 手动启动（开发模式）

```bash
# 1. 启动基础设施
docker-compose up -d rabbitmq mongodb mysql

# 2. 等待 MySQL 就绪后建表
mysql -h 127.0.0.1 -P 3307 -u root -p123456 media_db < backend/sql/schema.sql

# 3. 启动后端服务
cd backend
cargo run --release
```

## Services

| 服务 | 端口映射 | 说明 |
|------|----------|------|
| backend | 9999 | Rust 微服务（健康检查端口） |
| mysql | 3307 → 3306 | MySQL 5.7 数据库 |
| rabbitmq | 5672 / 15672 | RabbitMQ 消息队列（管理面板 15672） |
| mongodb | 27017 | MongoDB 4.0.13 剧本数据存储 |

## 测试账号

| 服务 | 用户名 | 密码 |
|------|--------|------|
| MySQL | root | 123456 |
| RabbitMQ | guest | guest |
| MongoDB | （无认证） | — |

健康检查端点：
- `GET /healthz`、`GET /readyz`：返回依赖项状态，全部正常返回 `200`，异常返回 `503`
- `GET /metrics`：返回 Prometheus 文本指标

## 题目内容

生成一个生产级、健壮、模块化的 Rust 异步微服务代码框架，使用 tokio 作为运行时，所有 I/O 操作必须是非阻塞的、最小第三方依赖原则。

核心功能：
- 监听 RabbitMQ 队列（手动 ACK 模式），消息格式为 `{"task_id": 1234}`
- 收到消息后，从 MongoDB（v4.0.13）查询 task_id 对应的剧本 JSON（结构含"第N场"和"分镜"列表）
- 按依赖顺序调用外部 LLM HTTP API（配置化 URL、模型名、API Key）生成 6 类提示词
- 每个 LLM 调用失败时，根据 HTTP 状态码决定是否重试（4xx 不重试，5xx/网络错误最多重试 3 次，指数退避）
- 若场级提示词失败，整个任务失败；若分镜内任一提示词失败，停止该分镜后续生成，但记录已成功字段和错误原因
- 将结果写入 MySQL 5.7 表 tb_media_prompt
- 成功后向下一队列发送消息并 ACK；失败则 NACK(requeue=false) 并丢弃
- 记录每个 LLM 调用的耗时、token 消耗、错误到结构化日志
- 通过配置文件（YAML）控制所有连接参数、最大并发任务数、最大 LLM 并发数

技术架构：

```
RabbitMQ ──► Consumer ──► MongoDB Query ──► LLM Pipeline ──► MySQL Write ──► Producer ──► ACK
                │                              │
          Semaphore(task)              Semaphore(llm)
```

模块划分：

- **config** — YAML 配置反序列化，支持环境变量覆盖
- **error** — 统一错误类型
- **mq** — RabbitMQ 消费者/生产者
- **db** — MongoDB 查询 + MySQL 批量写入
- **llm** — HTTP 客户端，重试策略，指标采集
- **task** — 任务编排、场处理、分镜状态机
- **model** — 数据模型定义
