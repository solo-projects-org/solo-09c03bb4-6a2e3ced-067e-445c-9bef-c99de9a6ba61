# geocache — 冷档砷含量查询服务

地质调查员按**钻孔号 + 半开深度区间 `[depth_min, depth_max)`** 查询冷档（cold
archive）中的砷含量，定位待复查岩芯。单实例 API 以固定清单（manifest）描述
不可变 Parquet 档案，带一个字节预算受限的热缓存，缓存前置在一个可慢传、可
断流的本地冷档源之前。

## 组成

| 组件 | 说明 |
|---|---|
| `geocache/manifest.py` | 固定清单模型：每档含钻孔集合、深度范围、URL、字节数、SHA-256，启动时加载，不可变 |
| `geocache/cache.py` | 热缓存：字节预算（含下载临时文件）、单飞下载、读钉（pin）、LRU 驱逐、启动清理 |
| `geocache/fetcher.py` | 下载 + 长度/SHA-256 校验，失败重试，半档删除 |
| `geocache/parquet_ops.py` | PyArrow 逐批过滤（`ParquetFile.iter_batches` + compute 掩码），输出 NDJSON 行 |
| `geocache/main.py` | FastAPI：`GET /query`、`/manifest`、`/cache/stats`、`/health`，Swagger 在 `/docs` |
| `geocache/coldstore.py` | 本地冷档源：慢传（`CHUNK_DELAY_MS`）与断流（`CUT_AFTER_BYTES`/`CUT_TIMES`）故障注入 |
| `geocache/datagen.py` | 一次性生成演示档案（6 个不可变 Parquet）与 `manifest.json` |

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

* API + Swagger UI: <http://localhost:8000/docs>
* 冷档源: <http://localhost:8001>（`/files/{name}`、`/stats`）

`docker-compose.yml` 把 `CACHE_CAPACITY_BYTES` 设为 27103：能容纳最大的两个
单档，但装不下 BH-001 全区间查询所需的三个档（约 39 KB）——容量可以小于
查询所需总量，查询逐档流式处理、边走边驱逐。

### 演示查询

```bash
# 钻孔 BH-001，半开区间 [20, 120)：跨 core_A/core_B/core_C 三档
curl -N "http://localhost:8000/query?borehole=BH-001&depth_min=20&depth_max=120"

# 带终态校验的客户端（未见 complete 终态即未完成，退出码非 0）
python scripts/query_client.py BH-001 20 120

# 缓存占用（含下载临时文件预留）
curl -s http://localhost:8000/cache/stats
```

NDJSON 每行一条记录 `{"borehole","depth","arsenic"}`，**末条**为终态：
`{"status":"complete","rows":N,"files":[...]}` 或
`{"status":"failed","error":...,"rows":N}`。

### 演示故障注入（改 `docker-compose.yml` 中 coldstore 的环境变量）

```yaml
# 每个文件第一次请求传到 8000 字节即断流，之后正常 —— 演示重试成功
CUT_AFTER_BYTES: "8000"
CUT_TIMES: "1"
```

```yaml
# 每次请求都断流 —— 查询以 failed 终态结束，半档清除、不计命中
CUT_AFTER_BYTES: "8000"
CUT_TIMES: "0"
```

### 演示重启恢复

```bash
docker compose restart api   # 启动扫描：删半档临时文件，重校验幸存档（长度+SHA-256）
```

## 缓存设计要点

* **占用不超限**：`used_bytes` 计入在途下载的临时文件（按清单字节数预先
  预留），任何时刻 `used_bytes ≤ capacity`；容量必须 ≥ 最大单档（启动校验）。
* **单飞取回**：同一档的并发查询共享一次下载；下载在独立任务中运行，
  客户端断开不会打断他人仍需的下载。
* **读档不回收**：查询只钉住（pin）正在读的那一个档；驱逐只针对未钉的
  ready 档（LRU）。读取经打开的文件句柄进行，即便路径被 unlink 也不影响
  在读句柄（POSIX 语义）。
* **无互等死锁**：查询自身从不预留空间，只有单飞下载任务逐档预留；等空间
  只发生在下载任务里，读者总能读完释放，腾出空间后条件变量唤醒继续。
  等空间不阻塞缓存命中（命中只在锁内自增 pin）。
* **失败/重启清理**：下载失败或校验不符即删半档、释放预留、不计命中；
  重启时启动扫描做同样清理并重校验幸存档。

## 测试

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest
```

17 个用例覆盖：单飞与断连续传、读钉防驱逐、等空间唤醒且命中不阻塞、
多档查询无死锁、临时文件计入预算、失败清理与重试、重启扫描、半开区间
边界、NDJSON 终态行（complete/failed）、容量小于查询总量仍完成。

## 环境变量（API）

| 变量 | 默认 | 说明 |
|---|---|---|
| `MANIFEST_PATH` | `/data/manifest.json` | 固定清单路径 |
| `CACHE_DIR` | `/cache` | 热缓存目录 |
| `CACHE_CAPACITY_BYTES` | `0` = 2×最大单档 | 热缓存字节预算（含临时文件） |
| `FETCH_ATTEMPTS` | `2` | 每档下载尝试次数 |
| `HTTP_TIMEOUT_SECONDS` | `30` | 冷档请求超时 |
