# 整批交接内存优化 — 状态说明

## 改动

下载→接纳→加工→入账链路改为有界内存的文件交接，旧对象接口全部保留：

- **pipeline.py**（追加，未改旧函数）：新增内部流式文件交接接口。
  `open_text`（按 `json.loads(bytes)` 的编码探测解码，不改行尾）、
  `scan_layout`（第一遍：完整 JSON 语法 + 成员布局，末值语义）、
  `validate_head`/`check_row`（与 `validate` 同序同错的批次/行校验）、
  `absorb_rows`（第二遍：只校验获胜 rows 数组，逐行规范编码）、
  `Versions`（磁盘暂存 (id,rev) 库，有界缓存，rev 以十进制文本保存任意精度；
  完成/拒绝/异常均 cleanup 删除）、`stage_versions`（按 id 的 Python
  字符串序流式输出最高修订存活行，墓碑删除，精确整数 value*scale）。
- **runtime.py**：新增 `submit_file(root, raw_path)`——完整验证（全文档语法、
  批次头、全部获胜行、同修订冲突）后，才把与 `encode(batch)` 逐字节一致的
  规范输入落盘并登记 jobs；幂等、BATCH_CONFLICT、ACTIVE_BATCH 与入账事务
  边界同 `submit`。`run_one` 改从持久规范输入流式重校验、归约、stage；
  `summarize`/结果落盘/`commit_result`/回执原样。`submit`（对象接口）未动。
- **collector.py**：`run_one` 用 `runtime.submit_file(root, complete)` 替代
  整批 `json.loads`+对象 `submit`；原文 `downloads/TASK.raw`、下载状态机、
  重排队/失败分类不变。
- **regression.py**：新增 `boundary`（无效尾行拒绝、尾行同修订冲突、
  墓碑/复活/零和非空组/任意精度整数/精确重复行/非 ASCII 的差分、
  被覆盖 rows 成员、拒绝后可恢复重采、交接目录无残留暂存），
  `failure_retry` 增补入账失败后暂存清理断言。reference.py、bench.py、
  checks.py、jsonstream.py、portal.py 未动。
- **.pointwise-runtime.json**：verify 改为 `regression.py target`。

## 实测（本机，tracemalloc 新增 Python 峰值）

| 负载 | 行数 | 优化前 | 优化后 | 门槛 |
|---|---|---|---|---|
| distinct | 100000 | 96.8 MiB | 0.28 MiB | ≤32 MiB |
| distinct | 400000 | 387.2 MiB | 0.68 MiB | ≤32 MiB，增量 ≤4 MiB（实测 0.40） |
| hot | 100000 | 103.2 MiB | 0.18 MiB | ≤32 MiB |
| hot | 400000 | 413.2 MiB | 0.58 MiB | ≤32 MiB，增量 ≤4 MiB（实测 0.40） |

`bench.py target` 输出 FULL_CHAIN_TARGET_OK。sample 规范输入/结果 SHA256
与契约冻结值一致（d324a8b6…/2bd70ff9…）。另以 40 例随机差分（重复成员、
Unicode、大整数、乱序修订）与 30 例拒绝差分对冻结 reference.py 逐字节/
逐错误核对通过。

## 运行

- `python3 -B regression.py target`：全部回归 + 四类全链路基准（含峰值门槛）。
- `python3 -B regression.py quick`：不含基准的快速回归。
- `python3 -B bench.py demo`：真实下载至入账演示（状态、原文/规范输入摘要、结果、回执）。
