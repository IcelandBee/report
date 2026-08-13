# Doris 当前部署配置与使用交接归档

> 归档日期：2026-08-13  
> 归档范围：当前 Apache Doris 单机实例的位置、配置、运行状态、基本操作与已经完成的基础验证。  
> 不包含：后续 CLIP 向量检索、动态 `meta` 治理、数据模型和具体对比实验设计。

## 1. 当前结论

Apache Doris 已在目标服务器上完成单 FE、单 BE 部署，并完成以下基础验证：

- FE 和 BE 均能正常启动；
- BE 已注册到 FE，状态为 `Alive=true`；
- FE/BE 实际构建版本均为 `doris-4.1.3-rc02-7126cf65d96`；
- SQL 接口可用；
- 已成功创建测试数据库、设置配额、创建单副本表、插入并查询 1 行数据；
- BE 的 128 GiB 进程内存上限已正确生效；
- Doris 当前实际数据目录占用约 84 MiB；
- 尚未导入图片、CLIP 向量或动态 `meta` 业务数据。

当前实例可作为后续 Doris 对比实验的基础环境，但正式写入实验数据前应阅读本文的“已知限制与注意事项”。

## 2. 目标服务器概况

| 项目 | 当前值 |
|---|---|
| 操作系统 | Ubuntu 20.04.6 LTS |
| 内核 | `5.4.0-216-generic` |
| 主机名 | `camera` |
| 登录用户 | `camera`（UID 1000，属于 `sudo` 组） |
| 服务器地址 | `10.50.113.170/24` |
| 活跃网卡 | `ens1f0`，10 GbE，全双工，MTU 1500 |
| CPU | 2 × Intel Xeon Gold 6240，36 物理核、72 逻辑 CPU |
| NUMA | 2 个节点 |
| 内存 | 约 503 GiB |
| Swap | `/swap.img`，8 GiB；未关闭 |
| Doris 数据所在文件系统 | `/dev/sda`，ext4，挂载到 `/DATA` |
| `/DATA` 当前状态 | 约 42 TiB，总使用率约 97%，剩余约 1.3 TiB |

服务器上同时运行 Python、VLLM、Milvus、Node 等其他负载，因此它不是 Doris 独占机器。后续性能结果必须注明共享负载、磁盘高使用率和缺少完整 I/O 指标等背景，不宜直接作为生产容量结论。

## 3. 目录与文件位置

### 3.1 Doris 根目录

```text
/DATA/h30082292/doris/
├── archive/       # 配置备份和环境变更审计记录
├── be-storage/    # BE 内部表数据、tablet、WAL、meta 等
├── fe-meta/       # FE 元数据
├── log/           # 预留目录；当前 FE/BE 主日志不在这里
├── software/      # Doris 与便携 JDK
└── tmp/           # 预留临时目录
```

目录所有者为 `camera:camera`，主要目录权限为 `750`。

### 3.2 软件位置

```text
Doris Home:
/DATA/h30082292/doris/software/apache-doris-4.1.3-bin-x64

JDK 17 Home:
/DATA/h30082292/doris/software/jdk-17.0.19+10
```

JDK 为 Temurin OpenJDK 17.0.19+10。Doris 与 JDK 压缩包均在部署前完成了校验。

### 3.3 关键配置文件

```text
FE 配置：
/DATA/h30082292/doris/software/apache-doris-4.1.3-bin-x64/fe/conf/fe.conf

BE 配置：
/DATA/h30082292/doris/software/apache-doris-4.1.3-bin-x64/be/conf/be.conf
```

### 3.4 实际日志位置

```text
FE 日志：
/DATA/h30082292/doris/software/apache-doris-4.1.3-bin-x64/fe/log/

BE 日志：
/DATA/h30082292/doris/software/apache-doris-4.1.3-bin-x64/be/log/
```

常用日志包括：

```text
fe/log/fe.log
fe/log/fe.warn.log
fe/log/fe.audit.log
fe/log/fe.out

be/log/be.INFO
be/log/be.WARNING
be/log/be.ERROR
be/log/be.out
```

具体日志文件名可能包含日期，`be.INFO`、`be.WARNING` 等通常是指向当前日志文件的软链接。

### 3.5 配置备份与审计记录

首次启动前的配置和后续关键变更保存在：

```text
/DATA/h30082292/doris/archive/config-before-first-start/
/DATA/h30082292/doris/archive/environment/
```

其中包括：

- FE/BE 原始配置；
- BE 存储容量语法修改前备份；
- BE 内存限制语法修改前备份；
- `vm.max_map_count` 临时修改记录；
- 保持 Swap 开启并跳过启动检查的审计记录。

## 4. 当前 Doris 拓扑与端口

当前为一体化存算、单 FE、单 BE，仅用于验证，不具备高可用能力。

### 4.1 FE

| 项目 | 当前值 |
|---|---|
| 地址 | `10.50.113.170` |
| 角色 | `FOLLOWER`，当前为 Master |
| HTTP 端口 | `8030` |
| MySQL Query 端口 | `9030` |
| RPC 端口 | `9020` |
| Edit Log 端口 | `9010` |
| Arrow Flight SQL 端口 | `8070` |

### 4.2 BE

| 项目 | 当前值 |
|---|---|
| Backend ID | `1786530706047` |
| 地址 | `10.50.113.170` |
| Heartbeat 端口 | `9050` |
| BE Thrift 端口 | `9060` |
| HTTP 端口 | `8040` |
| BRPC 端口 | `8060` |
| Arrow Flight SQL 端口 | `8050` |
| Node Role | `mix` |

BE 注册命令为：

```sql
ALTER SYSTEM ADD BACKEND "10.50.113.170:9050";
```

该命令已经执行完成，不要重复注册。

## 5. 当前生效配置

### 5.1 FE 关键配置

```ini
JAVA_HOME=/DATA/h30082292/doris/software/jdk-17.0.19+10
meta_dir = /DATA/h30082292/doris/fe-meta
priority_networks = 10.50.113.0/24
```

FE 使用安装包中的 JDK 17 JVM 参数，当前堆内存为：

```text
-Xms8192m
-Xmx8192m
```

### 5.2 BE 关键配置

```ini
JAVA_HOME=/DATA/h30082292/doris/software/jdk-17.0.19+10
priority_networks = 10.50.113.0/24
storage_root_path = /DATA/h30082292/doris/be-storage,medium:hdd,capacity:50
mem_limit = 128G
```

重要语法说明：

- `mem_limit` 必须写成 `128G`。当前 4.1.3 构建不能解析 `128GB`；写成 `128GB` 会导致实际内存上限为 `-1`，进而使 BE 查询失败。
- `capacity:50` 中的 `50` 按 GiB 解析，但当前观测表明它没有把 `/DATA/h30082292/doris/be-storage` 变成独立 50 GiB 文件系统配额。Doris 指标和 `SHOW BACKENDS` 仍按整个 `/dev/sda` 文件系统报告总容量和使用率。

### 5.3 CPU 绑定

当前进程启动时采用：

```text
FE CPU：0-3,36-39
BE CPU：4-17,40-53
```

对应启动方式使用 `taskset`。这只是 CPU affinity，不是完整的资源隔离；服务器上的其他进程仍可能在这些 CPU 上运行。

### 5.4 内存与 Swap

- FE Java Heap：8 GiB；
- BE `mem_limit`：128 GiB；
- BE 启动日志已确认 `Mem Limit: 128.00 GB`；
- Swap 保持启用，当前未执行 `swapoff`；
- 观察期间 `vmstat` 的 `si/so` 为 0，但后续性能实验仍需持续记录；
- BE 启动脚本默认拒绝在 Swap 开启时启动，因此当前采用单次环境变量 `SKIP_CHECK_ULIMIT=true` 绕过启动脚本检查；这不会改变 Swap 状态。

## 6. 宿主机临时参数

BE 要求：

```ini
vm.max_map_count=2000000
```

服务器原值为：

```ini
vm.max_map_count=65530
```

本次使用以下方式进行了宿主机范围的临时修改：

```bash
sudo sysctl -w vm.max_map_count=2000000
```

该修改：

- 对整台宿主机生效，不只针对 Doris；
- 没有写入 `/etc/sysctl.conf` 或常见持久化配置；
- 主机重启后通常会恢复原值；
- 审计记录中计划在实验结束、BE 停止后恢复到 `65530`。

不要在 BE 运行期间直接恢复该参数。若要恢复，必须先停止 BE、确认没有其他程序依赖当前值，并经过服务器负责人确认。

## 7. 启动、停止与状态检查

以下命令均以 `camera` 用户执行。

### 7.1 启动 FE

```bash
DORIS_HOME=/DATA/h30082292/doris/software/apache-doris-4.1.3-bin-x64

taskset -c 0-3,36-39 \
  "$DORIS_HOME/fe/bin/start_fe.sh" --daemon
```

### 7.2 启动 BE

启动前必须确认：

```bash
cat /proc/sys/vm/max_map_count
ulimit -n
swapon --show
```

当前预期分别为：

```text
vm.max_map_count = 2000000
ulimit -n = 1048576
Swap 仍启用
```

当前启动命令：

```bash
DORIS_HOME=/DATA/h30082292/doris/software/apache-doris-4.1.3-bin-x64

SKIP_CHECK_ULIMIT=true \
taskset -c 4-17,40-53 \
  "$DORIS_HOME/be/bin/start_be.sh" --daemon
```

`SKIP_CHECK_ULIMIT=true` 会跳过启动脚本中包括 Swap、`vm.max_map_count` 和文件句柄在内的检查，所以使用前必须人工确认后两项仍满足要求。

### 7.3 停止顺序

建议先停止 BE，再停止 FE：

```bash
DORIS_HOME=/DATA/h30082292/doris/software/apache-doris-4.1.3-bin-x64

"$DORIS_HOME/be/bin/stop_be.sh"
"$DORIS_HOME/fe/bin/stop_fe.sh"
```

不要直接 `kill -9`。只有正常停止长时间无效、且已经检查日志和获得确认后，才考虑强制终止。

### 7.4 进程、端口和健康检查

```bash
pgrep -af 'org\.apache\.doris\.DorisFE'
pgrep -af '[/]be/lib/doris_be'

ss -lntp | grep -E ':(8030|8040|8050|8060|8070|9010|9020|9030|9050|9060)[[:space:]]'

curl --max-time 5 http://127.0.0.1:8040/api/health
```

BE 健康接口正常响应为：

```json
{"status": "OK", "msg": "OK"}
```

### 7.5 常用日志检查

```bash
DORIS_HOME=/DATA/h30082292/doris/software/apache-doris-4.1.3-bin-x64

tail -n 100 "$DORIS_HOME/fe/log/fe.log"
tail -n 100 "$DORIS_HOME/be/log/be.INFO"
tail -n 100 "$DORIS_HOME/be/log/be.WARNING"
tail -n 100 "$DORIS_HOME/be/log/be.out"
```

## 8. SQL 连接与简单使用

### 8.1 连接方式

服务器目前没有安装 MySQL CLI，也没有安装 PyMySQL。当前主要通过 FE HTTP SQL API 操作。

接口地址：

```text
http://127.0.0.1:8030/api/query/default_cluster/information_schema
```

当前使用 Doris `root` 用户和空密码，仅适用于当前受控测试环境。若服务端口可被其他机器访问，应尽快设置密码并检查防火墙策略。

通用 Bash 函数：

```bash
FE_SQL_URL='http://127.0.0.1:8030/api/query/default_cluster/information_schema'

sql_request() {
    local statement=$1

    curl --max-time 30 -sS \
        -u 'root:' \
        -H 'Content-Type: application/json' \
        -X POST "$FE_SQL_URL" \
        --data "$(python3 -c \
            'import json,sys; print(json.dumps({"stmt":sys.argv[1]}))' \
            "$statement")"
    printf '\n'
}
```

示例：

```bash
sql_request 'SHOW FRONTENDS'
sql_request 'SHOW BACKENDS'
sql_request 'SHOW DATABASES'
sql_request 'SELECT VERSION()'
```

HTTP API 返回中必须检查：

```json
"msg": "success",
"code": 0
```

HTTP 200 本身只表示请求到达 FE，不代表 SQL 一定执行成功。

### 8.2 MySQL 协议连接

若另一台机器已有 MySQL 客户端，并且网络与防火墙允许访问 9030，可使用：

```bash
mysql -h 10.50.113.170 -P 9030 -uroot
```

当前 `root` 密码为空。不要在未确认网络暴露范围的情况下长期维持空密码。

### 8.3 版本识别

```sql
SELECT VERSION();
```

当前返回：

```text
5.7.99
```

这是 Doris 暴露的 MySQL 兼容版本，不是安装包版本。实际构建版本应通过以下命令查看：

```sql
SHOW FRONTENDS;
SHOW BACKENDS;
```

当前 FE/BE 实际版本均为：

```text
doris-4.1.3-rc02-7126cf65d96
```

## 9. 当前测试数据库

### 9.1 对象清单

```text
数据库：camera_doris_validation
数据配额：10 GiB
表：camera_doris_validation.smoke_test
副本数：1
Bucket 数：1
当前行数：1
```

数据库配额设置命令为：

```sql
ALTER DATABASE camera_doris_validation
SET DATA QUOTA 10737418240;
```

当前测试表定义的核心部分：

```sql
CREATE TABLE camera_doris_validation.smoke_test (
    id BIGINT NOT NULL,
    note VARCHAR(64) NULL,
    created_at DATETIME NOT NULL
)
ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
```

当前唯一一行数据：

```text
id = 1
note = doris smoke test
created_at = 2026-08-13 06:31:28
```

### 9.2 基础查询

```sql
SELECT *
FROM camera_doris_validation.smoke_test
ORDER BY id;

SELECT COUNT(*)
FROM camera_doris_validation.smoke_test;
```

### 9.3 查看空间和配额

数据库级信息需要先切换数据库：

```sql
USE camera_doris_validation;
SHOW DATA;
```

当前结果：

```text
smoke_test：863 B
Total：863 B
Quota：10.000 GB
Left：10.000 GB（显示精度导致看起来未减少）
```

查看指定表：

```sql
SHOW DATA FROM camera_doris_validation.smoke_test;
```

不要使用：

```sql
SHOW DATA FROM camera_doris_validation;
```

因为 `FROM` 后面要求的是表名，单独传数据库名会被当成表名并返回 `table not found`。

## 10. 已完成的验证证据

### 10.1 节点状态

最后一次检查时：

```text
FE Alive：true
FE IsMaster：true
BE Alive：true
BE HeartbeatFailureCounter：0
BE RunningTasks：0
```

### 10.2 内存限制

修正 `mem_limit = 128G` 并重启后，BE 启动日志明确记录：

```text
Mem Limit: 128.00 GB
origin config value: 128G
Memory Limt: 137438953472
```

此前使用 `128GB` 时日志为：

```text
Failed to parse mem limit from '128GB'
Mem Limit: -1.00 B
```

该错误已经修复。

### 10.3 最小写入

已经依次验证：

1. 创建数据库成功；
2. 设置 10 GiB 数据配额成功；
3. 创建单副本、单 Bucket OLAP 表成功；
4. 插入 1 行成功；
5. 查询该行成功；
6. `COUNT(*)` 返回 1；
7. 表级统计显示 863 B、1 个副本、1 行。

当前 BE 的 tablet 数量由 22 增加到 23。其中原有 22 个属于 Doris 自动创建的 `__internal_schema`，新增 1 个属于 `smoke_test`。

## 11. 已知限制与注意事项

### 11.1 `/DATA` 使用率高

当前 Doris 报告：

```text
TotalCapacity：约 41.747 TiB
AvailCapacity：约 1.234 TiB
UsedPct / MaxDiskUsedPct：约 97.04%
```

FE 默认磁盘阈值：

```text
storage_high_watermark_usage_percent = 85
storage_min_left_capacity_bytes = 2 GiB
storage_flood_stage_usage_percent = 95
storage_flood_stage_left_capacity_bytes = 1 GiB
```

BE 默认 Flood Stage：

```text
storage_flood_stage_usage_percent = 90
storage_flood_stage_left_capacity_bytes = 1 GiB
```

当前使用率已超过百分比阈值，但剩余空间仍远大于 1 GiB，因此基础写入已成功。不过高水位会影响平衡、迁移等操作。不要为了实验随意提高阈值；任何阈值修改都应先评估公共服务器风险并获得确认。

### 11.2 `capacity:50` 不是可靠硬配额

虽然 BE 配置为：

```ini
storage_root_path = /DATA/h30082292/doris/be-storage,medium:hdd,capacity:50
```

但运行时指标仍报告整个 `/dev/sda` 的约 42 TiB 容量和 97% 使用率。它不能代替 ext4 project quota、独立逻辑卷、独立文件系统或容器级磁盘配额。

当前真正对实验数据提供应用层约束的是：

```text
camera_doris_validation 数据库 10 GiB DATA QUOTA
```

该配额也不是操作系统硬配额，且 FE 默认按周期更新已用数据量；不要把它视为防止宿主盘写满的唯一手段。

### 11.3 NFS 不用于 Doris 内部存储

服务器挂载了：

```text
/mnt/DATA_71 -> 10.50.113.66:/DATA（NFSv4）
```

该路径可用于读取现有实验输入文件，但当前没有作为 Doris `storage_root_path`。不要未经评估就将 Doris 内部 tablet、WAL 或 FE 元数据迁移到 NFS。

### 11.4 性能隔离不完整

- FE/BE 只有 CPU affinity；
- 没有独占 NUMA 节点；
- 其他进程仍可能使用相同 CPU 和 NUMA 内存；
- `numactl`、`iostat` 当前未安装；
- RAID 控制器为 Broadcom/LSI MegaRAID SAS3508，但底层盘型、RAID 级别和条带等信息尚未完全确认；
- 系统时钟的 NTP 同步状态曾显示 `System clock synchronized: no`。

因此，后续对比实验至少应记录当时的共享负载、CPU、RSS、Swap `si/so`、目录占用和 Doris Profile，并明确标注这是共享服务器上的 PoC 数据。

### 11.5 安全性

- Doris `root` 当前为空密码；
- FE/BE 服务监听在 `0.0.0.0`；
- UFW 为 active，但当前用户未取得完整规则列表；
- AppArmor active/enabled。

在扩大访问范围或长期运行前，应由服务器负责人确认端口暴露，并为 Doris 管理员设置密码。本文不包含防火墙或认证修改命令，因为这会影响共享服务器和后续连接方式。

## 12. 为后续同配置对比保留的资源基线

若同事部署 ClickHouse 进行对比，至少应在实验记录中对齐或明确说明以下 Doris 基线；本节只记录当前值，不规定 ClickHouse 的具体部署方案：

| 维度 | 当前 Doris 基线 |
|---|---|
| 服务器 | `10.50.113.170` 共享服务器 |
| 计算资源 | BE affinity：`4-17,40-53`；FE affinity：`0-3,36-39` |
| BE 内存上限 | 128 GiB |
| FE Java Heap | 8 GiB |
| 实验数据应用层配额 | 10 GiB（当前基础库） |
| 数据文件系统 | `/dev/sda` ext4，挂载 `/DATA` |
| 数据目录 | `/DATA/h30082292/doris/be-storage` |
| 副本数 | 1 |
| 基础表 Bucket | 1 |
| 网络 | 10 GbE，MTU 1500 |
| Swap | 开启，实验期间持续观察 `si/so` |
| 共享负载 | Python、VLLM、Milvus、Node 等 |

对比实验不应复用 Doris 的 `be-storage`、`fe-meta` 或端口；ClickHouse 应使用独立目录和不冲突的端口，并单独确认磁盘空间与权限。

## 13. 交接后的建议检查顺序

同事接手后，在执行任何实验写入前，建议按以下顺序检查：

1. `vm.max_map_count` 是否仍为 2000000；
2. FE、BE 进程是否存在；
3. FE/BE 端口和 BE `/api/health` 是否正常；
4. `SHOW FRONTENDS` 和 `SHOW BACKENDS` 是否均为 Alive；
5. BE 启动日志是否仍显示 `Mem Limit: 128.00 GB`；
6. `/DATA` 剩余空间及使用率是否显著变化；
7. `vmstat` 是否出现持续 `si/so`；
8. `camera_doris_validation.smoke_test` 是否仍能查询到 1 行；
9. 再由实验负责人设计和创建后续专用表、向量索引及动态属性结构。

本归档到此为止。后续 CLIP、版本查询和动态 `meta` 的具体实验步骤、数据生成、索引参数及验收口径由实验负责人另行设计和记录。
