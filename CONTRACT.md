# 门户导出内存优化的公开契约

现有业务实现为 pipeline.py、runtime.py、collector.py；portal.py 是可控的 HTTP 录制源，checks.py 是原有回归，sample.json 是完整输入。本题只提供这些业务依赖，原题源中的安装、代理和卷实验设施不属于本题交付范围。Python 标准库及 SQLite，无额外安装步骤。

## 调用与持久输出

下列均为真实 Python 调用，不通过 shell 包装。root、control 是调用方选定的路径：
- runtime.init(root, control)，随后 collector.init(root)：新建一套仓库，初始化仅调用一次。
- collector.enqueue(root, task_id, url)：登记下载，返回 task_id、url；相同任务同 URL 幂等，不同 URL 报 TASK_CONFLICT。最多一个未完成下载。
- collector.run_one(root)：推进 queued/downloading 下载，返回是否处理过任务；内部执行下载、完整接纳、加工、入账。完成后的原文保存为 downloads/TASK.raw，不能用规范输入替换原文。传输元数据 sha256 是原文字节摘要。
- runtime.submit(root, batch)：原有对象接口，先 validate 再接纳；返回 {"accepted":批次号,"input_sha256":规范输入摘要}。同批同规范内容幂等，异内容报 BATCH_CONFLICT；有其他未完成批次时报 ACTIVE_BATCH。
- runtime.run_one(root)：推进 pending/running 批次，返回是否处理过任务。
- runtime.status(root)：按批次号排序返回 batch_id、state、receipt；collector.status(root) 返回原下载状态字段。
- runtime.read(root, name)：未完成返回 {"batch_id":name,"available":false}；完成后另含 result、receipt，已发布结果摘要错误仍报 RESULT_CORRUPT。
- pipeline.validate(batch)、stage(batch, binary_output)、summarize(binary_source)、encode(value)、receipt(batch_id,input_hash,result_bytes) 的旧对象接口继续可用。

允许增加内部文件交接接口，并让采集链改用它们；旧对象接口的调用方已经持有对象，其预存输入不作为文件采集的内存指标。不改 jobs、ledger 的业务含义，不引入存储迁移。完整验证后才能新增业务任务，文件交接不能把未验证的前缀变成已接纳任务。

规范输入保存在 inputs/BATCH.json，为 encode(batch) 的全部字节，保留原顺序的所有 rows，不能仅保存获胜修订。加工结果保存在 results/BATCH.json，为 encode(result) 的全部字节。receipt 含 batch_id、input_sha256、result_sha256，两个摘要分别覆盖规范输入和结果；原文摘要与规范输入摘要不同是正常情况。ledger 每批恰有一行 result BLOB 和回执，完成任务状态与 ledger 仍同事务提交，重复采集或接纳不能重复入账。

## 数据与原有语义

下载的 HTTP 正文为 UTF-8 JSON，按现有 json.loads 解释，允许任意顶层成员次序、转义和空白；重复成员沿用末值语义。业务校验针对最终解析出的对象，已经被后续同名 rows 成员覆盖的旧数组不参与业务校验，但整份正文的 JSON 语法仍须完整有效。未规定只接受某一种排版。

batch 恰含 batch_id、scale、rows。批次号是非空 ASCII 字母、数字、下划线或连字符；scale 是 1～100 的 int，bool 不算 int；rows 为列表。每行恰含 id、rev、station、minute、value、deleted；id、station 是非空字符串，rev 是正 int，minute、value 是 int，deleted 是 bool。所有整数保持 Python 整数语义，不因 SQLite 的整数范围而收窄。

同 id/rev 的完整行必须相等；完全相同的重复行允许，即使有更高 rev，旧 rev 的同版冲突仍报 REVISION_CONFLICT。每个 id 取最高 rev，最高版墓碑删除该身份，复活通过更高非墓碑修订完成。stage 按 id 的 Python 字符串顺序输出存活行；汇总按 station/minute 排序，对 value*scale 求精确整数和并计存活身份数，零和非空组保留。空输入得到 []。

encode 固定为 ensure_ascii=False、sort_keys=True、紧凑分隔符、allow_nan=False 的 json.dumps，再加一个换行并编码 UTF-8。输入校验、业务结果、排序及上述拒绝语义以冻结 reference.py 和原回归共同核对。不能放宽格式或更换业务口径来降内存。

sample.json 的结果为 [{"station":"east","minute":0,"count":2,"sum":0},{"station":"west","minute":1,"count":1,"sum":70}]。其规范输入 SHA256 为 d324a8b60e617e898d3a6f7bdc36d1f5440749a4b454591500c8b2bf35e0e37d，结果 SHA256 为 2bd70ff978feeba307eff7bdfd945bf4dcd3a2411dd295cc5e274e61412dfb00。

## 异常与资源边界

保留原有单任务、单工作者、无并发手动加工的边界。传输短响应或 OSError 会将下载重新排队，格式及摘要 ValueError 将其标为 failed，其他原有异常传播行为保持。无效输入不得新增 jobs 或 ledger。已接纳后加工失败可保持 running，重试仍从有效持久输入重新加工；ledger 写入失败不能留下已完成状态或部分入账。新方案若改变内部暂存方式，正常完成、拒绝或异常退出时须关闭并清理其自建临时文件和数据库，保留原文、已接纳输入、已发布结果及调用方已有内容。这里不新增 SIGKILL 恢复协议、续传、并行下载或多工作者要求。

jsonstream.members(text_source) 仅提供通用增量 JSON 分帧，输出 (kind,key,value)，kind 为 value 或命名数组的 array/item/end；它不处理业务校验、重复顶层成员取舍、规范编码、修订归约或接纳发布，不能把它直接当成优化答案。源由调用方持有并关闭，单个 item 仍是普通 Python 值。

## 冻结基准和运行

reference.py 是全量内存参照，不包含目标优化。bench.py 的两类确定性负载为 distinct（大量不同身份）与 hot（8个身份的乱序修订），各有100000、400000行，单行大小有固定上界，最多256个结果组。基准源从独立文件返回不可变响应，沿用真实 collector.download；原 checks.source 继续核对 HTTPResponse 分帧、截断和坏摘要。

python3 -B checks.py verify：原有正常业务、录制源、截断及摘要回归。
python3 -B regression.py quick：原回归、解析工具和新增业务拒绝/重试回归。
python3 -B bench.py baseline：运行未设峰值门槛的四个完整链路基准，打印峰值、耗时、摘要及回执。
python3 -B regression.py baseline：全部回归及四个完整链路基准，作为初始基线核验。
python3 -B regression.py target：相同负载和断言，加上题面的峰值及增长门槛。
python3 -B bench.py demo：真实执行小样例的下载至入账，展示下载状态、原文摘要、规范输入摘要、结果及回执。

负载生成与全量参照计算在测量子进程外完成。每例使用新的子进程和仓库；初始化及登记之后、collector.run_one 开始前启动 tracemalloc，直到它返回才停止，覆盖下载、解析、验证、规范保存、加工和入账，不允许仅测 stage。报告 elapsed_seconds 供复跑观察，时间不是额外验收门槛。两类两档新增 Python 分配峰值各≤32MiB，同类400000行峰值减100000行峰值≤4MiB。不得整批或把全部身份/修订留在内存；用原生容器或额外进程隐藏全量驻留也不满足该要求，磁盘状态允许采用有界缓存。结果组和调用方必须返回的结果可按原接口持有。

保留 reference.py、bench.py、原 checks.py 的负载与原断言。补充差分、拒绝和新增资源清理核验，记录实际峰值及简短状态说明。初始 .pointwise-runtime.json 的 verify 运行 regression.py baseline，交付时改用 regression.py target，demo 继续展示真实采集结果；允许把自己的必要验证接入同一入口。运行清单 version=1、kind=cli，setup 为准备 argv 数组列表，verify/demo 各为单个 argv 数组，不能写 shell 命令字符串。
