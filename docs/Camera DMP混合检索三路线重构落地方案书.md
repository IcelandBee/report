# Camera DMP 混合检索三路线重构落地方案书

> 文档性质：检索层升级与重构实施方案  
> 适用范围：Camera DMP 当前 PostgreSQL + ClickHouse + OBS/MinIO + Milvus Lite 检索链路  
> 目标规模：1,000 万至 1 亿可检索 Asset/Segment  
> 部署约束：容器或虚拟机，不以 Kubernetes 为前置条件  
> 一致性目标：秒级最终一致  
> 切换方式：允许短暂停机，必须具备验证与回滚方案  
> 编制日期：2026-08-04

## 目录

1. 执行摘要
2. 目标、边界与验收口径
3. 当前平台问题诊断
4. 三条路线共同的前置重构
5. 路线一：ClickHouse 26.3 LTS 统一检索
6. 路线二：迁移到 Apache Doris 4.0
7. 路线三：ClickHouse 26.3 LTS + Qdrant
8. 三路线对比与决策矩阵
9. PoC 设计与决策门
10. 统一迁移、停机切换与回滚
11. 安全、观测与运维改造
12. 推荐实施路线
13. 官方资料与本地依据
14. 待实施前确认的数据

---

## 1. 执行摘要

### 1.1 建议结论

本次需要比较的三条路线均可建立混合检索，但三者实现的“混合”并不完全相同：

1. **ClickHouse 26.3 LTS 统一检索**最容易落地，也最利于减少组件。它可以在同一张表和同一条 SQL 中完成结构化过滤、Token 全文条件和 HNSW 向量排序，但 ClickHouse 26.3 的全文索引主要用于词项匹配和过滤，缺少原生 BM25 相关性评分与内置 RRF。因此，它更准确地属于“过滤型混合检索”，若要做关键词候选与向量候选的相关性融合，需要自建评分逻辑。
2. **Apache Doris 4.0**与两个目标的匹配度最高：同一引擎具备结构化过滤、倒排全文、BM25、ANN 向量索引，并可通过 SQL 实现 RRF。代价是需要整体迁移 ClickHouse，且 Doris 的 ANN 是 4.0 新增能力，索引构建、Compaction、冷启动和持续写入下的稳定性必须经过严格 PoC。
3. **ClickHouse + Qdrant**的向量检索能力、过滤能力和未来扩展能力最强，最容易实现高质量稠密/稀疏融合，但保留两个检索引擎，不能直接实现“减少组件”。它适合作为 Doris 向量性能或稳定性不达标时的性能优先路线。

三条路线只决定检索投影落在哪里，不能代替 DatasetVersion 治理。无论最终选择哪条路线，都必须先建设共同的版本控制面：不可变版本、Manifest、AssetRevision、成员关系、版本血缘、字段注册、导入断点和发布水位。该控制面由 PostgreSQL + OBS/MinIO 保存事实，再向候选检索引擎生成可重建投影。

综合“第一目标是混合检索、第二目标是减少组件”的优先级，建议采用以下决策顺序：

> **第一候选：Apache Doris 4.0，但必须通过生产等比例 PoC 决策门。**  
> **低风险回退：ClickHouse 26.3 LTS 统一检索。**  
> **性能优先回退：ClickHouse 26.3 LTS + Qdrant，但必须先证明其相对集中式 Milvus 的净收益。**

这不是建议立即将生产系统切换到 Doris，而是建议让 Doris 先获得“主候选资格”。如果 Doris 在持续写入、Compaction、冷启动或 1 亿级容量测试中不能达标，应停止迁移，转向 ClickHouse 26.3；只有在单引擎向量检索达不到召回率或延迟目标时，才评估专用向量服务。Qdrant 还需与集中式 Milvus 对照，未形成显著净收益时不执行替换。

### 1.2 三条路线的核心定位

| 路线 | 混合检索形态 | 检索产品数 | 最大价值 | 主要风险 |
|---|---|---:|---|---|
| ClickHouse 26.3 LTS | 结构化/Token 过滤 + ANN 排序；相关性融合需自建 | 1 | 改造最小、经验复用、组件最少 | 没有原生 BM25/RRF，向量与频繁更新需实测 |
| Apache Doris 4.0 | 结构化 + BM25 + ANN，可用 SQL RRF | 1 | 最接近单引擎三路相关性融合 | ANN 新、Compaction/预热风险、迁移面大 |
| ClickHouse + Qdrant | ClickHouse 全文/分析 + Qdrant 向量；网关或 Qdrant 内融合 | 2 | 向量检索最专门、扩展弹性最好 | 双库一致性、分页与融合、运维组件增加 |

---

## 2. 目标、边界与验收口径

### 2.1 本次“混合检索”的最低定义

第一阶段必须同时支持：

- **结构化过滤**：App、DatasetVersion、媒体类型、时间、设备、地域、质量状态、标签、权限等。
- **关键词/全文检索**：文件名、路径、人工描述、Caption、OCR、ASR、标签和业务文本。
- **CLIP 稠密向量检索**：文本搜图、以图搜图，以及后续视频关键帧或片段检索。
- **统一结果集**：返回同一套 `segment_id`，具有稳定排序、权限校验、去重和分页语义。

稀疏向量不作为第一阶段硬性要求，但允许在 Qdrant 路线中作为提升关键词相关性和融合质量的实现方式。

### 2.2 不在本次替换范围内的组件

- PostgreSQL：继续承担用户、App、Dataset、DatasetVersion、权限、工作流和索引状态等事务数据。
- OBS/MinIO：继续保存图片、视频、音频、文档、Parquet/JSON 和 Embedding 快照等原始或事实数据。
- RabbitMQ/Celery：第一阶段继续使用，不为架构形式额外引入 Kafka。
- Redis：是否保留由现有缓存和 Celery 结果后端依赖决定，不与数据库路线强绑定。

“单引擎”只表示检索数据落到一个数据库产品，不表示整个平台只剩一个数据库。

### 2.3 初始验收指标

以下数值是项目启动基线，最终应根据现网流量修订：

| 类别 | 初始门槛 |
|---|---|
| 权限与过滤 | 条件结果正确率 100%，跨 App/ACL 数据泄漏为 0 |
| ANN 质量 | 相对精确搜索的 Recall@10 ≥ 95%，Recall@50 ≥ 97% |
| 混合排序 | NDCG@10 不低于关键词或向量任一单路，目标提升 ≥ 5% |
| 简单结构化查询 | 目标并发下 P95 ≤ 500 ms |
| 三路混合查询 | 目标并发下 P95 ≤ 1 s、P99 ≤ 2 s |
| 索引新鲜度 | P95 ≤ 5 s、P99 ≤ 30 s |
| 删除传播 | 逻辑删除 P99 ≤ 30 s，物理回收异步执行 |
| 稳定性 | 连续 72 小时混合读写无不可恢复错误，错误率 < 0.1% |
| 可恢复性 | 任一检索索引可由统一快照和事件重新构建，不依赖原 Milvus Lite 文件 |

性能验收必须同时报告数据量、向量维度、过滤选择率、并发、冷热状态、Recall 和硬件，不能只给单一 QPS。

---

## 3. 当前平台问题诊断

### 3.1 当前检索形态

当前平台由以下链路组成：

- ClickHouse 24.3.2.23：每个 App 一张表，保存文件路径、类型、大小、`version_id[]`、`dataset_id[]`、`meta JSON`，执行结构化过滤、模糊检索、聚合和 H3 分析。
- Chinese-CLIP + Milvus Lite：执行文本搜图和以图搜图。
- 搜索服务：遍历多个 `.db` 文件，分别连接和检索，在 Python 中汇总、排序并按 `full_path` 去重。
- PostgreSQL：保存 DatasetVersion、业务关系和权限。
- OBS/MinIO：保存原始多模态文件。

因此当前不是统一混合检索，而是“ClickHouse 元数据检索 + 多个本地向量库 + 应用层拼接”。

### 3.2 必须优先消除的问题

#### 3.2.1 路径被当作数据身份

当前向量结果按 `full_path` 去重。文件移动、重命名或跨版本复用都会改变路径，但不会改变数据对象本身。三条路线都必须改为稳定的 `asset_id` 和 `segment_id`。

#### 3.2.2 向量索引被拆成大量本地文件

每次查询可能重新加载多个 Milvus Lite 数据库、建立线程池、逐库检索、关闭连接并在 Python 中合并。这会导致：

- 查询延迟随 DB 文件数量上升；
- 不能保证全局 ANN Top-K；
- 难以统一权限和 DatasetVersion 过滤；
- 无法形成集中备份、扩容、监控和索引生命周期；
- 模型、数据库连接和 API 请求竞争同一进程资源。

无论选择哪条路线，都应停止新增 Milvus Lite 文件，并将其导出为统一中间数据。

#### 3.2.3 每个 App 一张 ClickHouse 表

动态建表和动态 SQL 会不断增加 DDL、升级、Schema 管理和查询编排成本。目标模型应改为共享检索表，通过 `app_id`、分区、排序键、索引和资源治理实现隔离。

#### 3.2.4 缺少统一的索引提交协议

一个 DatasetVersion 同时依赖 PostgreSQL、ClickHouse、OBS 和向量索引。任一环节失败，都可能出现文件存在但不可检索、向量存在但版本已删除、权限已撤销但搜索仍可命中的情况。

#### 3.2.5 缺少真正可检索的文本

图片、视频和音频本身不能直接参加关键词检索。若只索引文件名和路径，三路混合检索的“全文”价值有限。平台需要逐步生成：

- 图片 Caption、OCR、标签；
- 视频关键帧 Caption、OCR、ASR、片段时间码；
- 音频 ASR、说话人和时间段；
- 文档正文、标题和章节；
- 统一标准化的业务字段和人工描述。

数据库路线不能替代内容理解与文本生成管道。

### 3.3 DatasetVersion 与数据治理问题

旧版《数据集版本管理设计》进一步暴露了六个当前仍需解决的问题。这些问题与检索引擎有关，但本质上属于数据集控制面、资产身份和 Schema 治理，不能依靠升级或更换数据库自动消失。

#### 3.3.1 数据集版本缺少正式模型

当前可以在 PostgreSQL、ClickHouse 字段或路径中记录版本标识，但缺少统一的版本生命周期、不可变规则、成员清单、父版本关系和发布条件。结果是“存在 `version_id`”却不能可靠回答：某个版本准确包含哪些数据、从哪个版本生成、是否已经冻结、是否可以回滚，以及索引是否与该版本完全一致。

#### 3.3.2 版本之间的物理数据与索引冗余边界不清

相同文件可能被多个数据集和版本复用。若每个版本复制完整文件、元数据和向量，会造成存储与索引膨胀；若所有版本直接共享并修改同一条记录，又会导致历史版本被新值污染。平台需要区分“物理内容复用”“逻辑版本成员关系”和“版本特定元数据”，而不是简单决定整行复制或整行去重。

#### 3.3.3 跨表和跨 App 检索困难

每个 App 独立 ClickHouse 表使跨 App 查询依赖动态 SQL、逐表查询或应用层合并。即便统一为共享表，跨 App 检索仍需要统一字段语义、租户权限、资源隔离和结果来源说明，不能只删除表边界。

#### 3.3.4 不同格式缺少统一的资产与检索表示

图片、视频、音频、文档、压缩包、BIN、NPZ 和 Parquet/JSON 具有不同的解析、切分和索引方式。如果只把路径和动态字段写进数据库，平台仍无法统一表达原始文件、衍生片段、可检索文本、向量和版本成员关系。

#### 3.3.5 不支持多级数据集与版本嵌套

使用 `dataset_path` 前缀表达层级，会把业务身份绑定到名称和目录；重命名、跨层复用、多父版本合并和血缘查询都会变得困难。数据集层级与版本血缘也不是同一种关系：前者通常是树，后者可能是多父节点的有向无环图，必须分别建模。

#### 3.3.6 不同数据集字段存在冲突与覆盖风险

JSON、VARIANT 或多张类型 KV 表只能解决“字段是否能保存”，不能解决同名字段的含义、类型、单位、来源和合并规则。若缺少字段契约，`speed`、`score`、`time`等字段可能在不同 App、数据集和算法中表达不同含义，跨 App 查询、聚合和版本合并会产生错误或静默覆盖。

### 3.4 旧版类型 KV 方案不宜直接沿用

旧设计使用 `dataset_kv_int/float/str/json/time`等 EAV/KV 表保存版本字段，再通过 `UNION ALL`、最新版本计算、Join 和聚合恢复一行数据。该设计在千万至一亿规模和混合检索场景下会带来行数放大、多表扫描、多次自连接、类型转换和复杂索引维护，也仍然依赖 `path`与`dataset_path`作为身份或层级。

因此本方案吸收旧设计提出的问题，但不直接采用其表结构。目标应是：**稳定资产身份 + 不可变版本 Manifest + 逻辑成员关系 + 当前检索投影 + 字段注册与治理**。

---

## 4. 三条路线共同的前置重构

这些工作与最终数据库无关，应优先实施，避免为每条路线重复改造。

### 4.1 统一资产与片段身份

建议定义：

| 字段 | 作用 |
|---|---|
| `asset_id` | 原始文件或逻辑资产的稳定 UUID |
| `segment_id` | 图片、视频片段、关键帧、音频片段、文档分块等检索单元 UUID |
| `embedding_id` | 某模型对某 Segment 生成的一次 Embedding 标识 |
| `entity_version` | 同一 Segment 的单调递增版本，用于幂等和乱序保护 |
| `model_id/model_version` | Embedding 模型与版本 |
| `embedding_dim` | 向量维度 |
| `normalized` | 是否已单位归一化 |
| `pipeline_run_id` | OCR、ASR、Caption、Embedding 生成批次 |

路径、OBS URI、Bucket 和目录都是属性，不再承担主键功能。

### 4.2 统一检索文档模型

所有路线都围绕同一逻辑模型设计：

```text
SearchDocument
├── segment_id / asset_id / app_id
├── dataset_id / dataset_version references
├── media_type / uri / path / file_name
├── searchable_text
│   ├── title
│   ├── caption
│   ├── ocr_text
│   ├── asr_text
│   └── tags
├── structured metadata
│   ├── capture_time / device_id / geo
│   ├── quality / status / source
│   └── hot fields + dynamic metadata
├── embedding + model metadata
├── permission_scope
└── entity_version / is_deleted / updated_at
```

`meta JSON`或 Doris `VARIANT`可以保存长尾字段，但高频过滤字段必须提升为正式列并建立索引。

### 4.3 Dataset 与 DatasetVersion 控制面

这一部分是三条数据库路线的共同前置方案，由 PostgreSQL 和 OBS/MinIO 保存事实，ClickHouse、Doris 或 Qdrant 只保存可重建的检索投影。

#### 4.3.1 最小领域模型

| 对象 | 关键字段 | 作用 |
|---|---|---|
| `dataset` | `dataset_id, app_id, parent_dataset_id` | 数据集身份、所属 App 和可选的树形组织关系 |
| `dataset_version` | `version_id, dataset_id, status, manifest_uri, schema_version` | 不可变版本句柄、状态和 Manifest 地址 |
| `dataset_version_parent` | `version_id, parent_version_id, relation_type` | 表达派生、合并等多父版本血缘，形成 DAG |
| `asset` | `asset_id, media_type, source_system, created_at` | 不随路径和内容修订变化的逻辑资产身份 |
| `asset_revision` | `revision_id, asset_id, content_hash, metadata_hash` | 内容或核心元数据发生变化后的不可变修订 |
| `segment` | `segment_id, revision_id, segment_type, time_range/page_no` | 图片、关键帧、音视频片段或文档分块 |
| `dataset_version_member` | `version_id, revision_id/segment_id, member_metadata_uri` | 版本包含哪些资产或片段及版本特定属性 |
| `field_definition` | `field_id, namespace, name, type, unit, merge_policy` | 字段语义、类型、单位、来源和合并规则 |
| `ingestion_run` | `run_id, version_id, status, checkpoint, error` | 导入、续写、失败恢复和审计 |

不要求第一阶段一次实现所有高级字段，但 `dataset_version`、`asset/revision`、`dataset_version_member`、`field_definition`和`ingestion_run`是解决旧问题的最低集合。

#### 4.3.2 不可变版本与状态机

建议使用以下基本状态：

```text
DRAFT → BUILDING → READY → PUBLISHED → DEPRECATED
             └────→ FAILED
DEPRECATED → DELETING → DELETED
```

- `DRAFT/BUILDING`期间允许追加成员和处理数据。
- 进入 `READY`前生成不可变 Manifest、成员数量、Schema Hash 和内容校验值。
- `READY/PUBLISHED`版本禁止原地修改；任何内容或字段变化都创建新版本。
- `PUBLISHED`前必须确认事实数据、成员清单和目标检索索引达到同一个版本水位。
- 回滚是把业务指针切回旧 `version_id`，不是修改旧版本的数据。
- 删除先撤销可见性并传播墓碑，物理文件和索引延迟回收。

#### 4.3.3 Manifest 与去重策略

每个版本在 OBS/MinIO 保存一份 Manifest，至少记录：

```text
version_id
parent_version_ids
schema_version
asset_revision/segment 列表或分片文件
对象URI与content_hash
成员级元数据引用
模型与处理Pipeline版本
row_count / checksum / created_at
```

去重规则明确为：

- 原始内容相同：通过 `content_hash`复用同一个物理 Blob；同一 Asset 内容未变化时继续复用原 AssetRevision，不同逻辑 Asset 可以共享 Blob 但保留各自身份。
- 同一内容属于多个版本：只新增 `dataset_version_member`，不复制原文件。
- 内容或不可共享的核心元数据变化：创建新的 AssetRevision，旧版本继续引用旧修订。
- Caption、OCR、ASR、Embedding 等衍生结果按 `segment_id + pipeline/model_version`保存，允许不同版本复用。
- 检索数据库中的重复投影可以为性能存在，但必须能从 Manifest 重建，不能成为唯一事实。

#### 4.3.4 多级数据集与版本血缘

- 数据集的目录/组织层级使用稳定 `dataset_id + parent_dataset_id`表达，不使用路径前缀作为身份。
- 版本派生关系使用 `dataset_version_parent`表达，支持一个版本由多个父版本合并生成。
- 对高频祖先/后代查询，可以维护 Closure Table 或物化关系投影，但原始父子边仍是事实来源。
- 检索时先把目标 `version_id`解析为成员范围，再交给检索引擎；不能要求 Qdrant 理解完整版本 DAG。

#### 4.3.5 字段注册与冲突处理

字段使用稳定 `field_id`和命名空间，例如：

```text
camera.vehicle_speed_kmh
video.frame_rate
quality.blur_score
model.inference_score
```

`field_definition`至少定义数据类型、单位、可空性、语义说明、来源、是否可检索/聚合和合并策略。合并策略可取：

- `IMMUTABLE`：创建后不可修改；
- `REPLACE`：新版本替换；
- `APPEND`：集合追加并去重；
- `MAX/MIN`：数值归并；
- `SOURCE_PRIORITY`：人工、规则和模型结果按优先级选择；
- `VERSIONED`：每个版本保存独立值，不向其他版本传播。

存储上不再为每种类型创建一张通用 KV 表。高频稳定字段使用正式列，长尾字段使用 ClickHouse JSON 或 Doris VARIANT，字段注册表负责约束与解释；Qdrant只复制向量预过滤需要的少量已治理字段。

#### 4.3.6 成员检索投影

不建议继续只依赖 `version_id[]`表达所有版本关系。检索侧统一生成：

```text
dataset_membership
  dataset_version_id
  app_id
  asset_id
  segment_id
  entity_version
  is_deleted
```

- ClickHouse 或 Doris 路线：成员投影与共享 SearchDocument 表在同一引擎，通过 Join、字典、物化视图或高频版本标签完成过滤。
- ClickHouse + Qdrant 路线：完整成员关系保留在 ClickHouse；Qdrant只物化高频且规模可控的版本标签，复杂版本过滤由 Search Gateway 编排。
- 对当前版本、小成员集合或高频条件，可以将版本标签物化到 SearchDocument/Payload。
- 对百万以上成员版本，不发送巨型 `IN (...)`；使用成员表 Join、路由、预计算标签或向量超采样后回到分析库校验。

#### 4.3.7 版本创建与发布流程

```text
创建DRAFT版本
  → 创建ingestion_run并分批解析原始数据
  → 以chunk_id幂等写Asset/Revision/Segment
  → 写成员关系并持续更新checkpoint
  → 生成Manifest、Schema Hash和Checksum
  → 版本进入READY并发出索引事件
  → 检索投影达到version watermark
  → 权限与抽样校验通过
  → 原子切换为PUBLISHED
```

失败后从 `ingestion_run.checkpoint`续写，不删除整个版本后重新开始。若重新同步，应创建新的构建批次或新版本；已发布版本不执行“删除后重写”。

#### 4.3.8 三条路线的职责边界

| 路线 | 版本事实 | 成员/字段检索投影 | 向量版本标签 |
|---|---|---|---|
| ClickHouse 26.3 | PostgreSQL + Manifest | ClickHouse | 同一 SearchDocument 或关联投影 |
| Apache Doris | PostgreSQL + Manifest | Doris | 同一 SearchDocument 或关联投影 |
| ClickHouse + Qdrant | PostgreSQL + Manifest | ClickHouse | Qdrant只保存必要 Payload，最终以ClickHouse校验 |

所以数据库选型不改变版本控制模型，只改变检索投影的落点与查询实现。

### 4.4 Outbox 与索引事件

业务事务与索引更新必须通过 PostgreSQL Outbox 解耦：

```text
index_event
  event_id
  entity_type
  entity_id
  operation          UPSERT / DELETE / REEMBED / ACL_CHANGE
  entity_version
  app_id
  occurred_at
  payload_uri
  payload_hash
```

处理流程：

```text
PostgreSQL事务提交
      │
      └─ 同事务写Outbox
              │
       Outbox Publisher
              │
         RabbitMQ/Celery
              │
       路线对应的Indexer
              │
      幂等Upsert/Delete
              │
      回写index_state状态
```

索引状态至少记录：

```text
entity_id
source_version
target_name
indexed_version
status
last_error
retry_count
updated_at
```

必须提供死信、人工重放、按 App 重建、按模型重建和全量对账工具。

### 4.5 Embedding Service 独立化

Chinese-CLIP 模型推理从搜索 API 中拆出：

- 在线 Query Embedding：低延迟、小批量，可按 GPU 服务扩容。
- 离线 Asset Embedding：Celery 批任务，大批量和断点续传。
- 每次输出都携带模型版本、维度、归一化方式和输入摘要。
- 新旧模型通过双索引/双列/双 Collection 并行，完成影子验证后切换。

### 4.6 Search API 统一化

建议新建 `/v2/search`：

```json
{
  "app_id": "...",
  "query_text": "夜间道路积水",
  "query_image": null,
  "dataset_version_id": "...",
  "filters": {
    "media_type": ["image", "video_frame"],
    "capture_time": {"gte": "2026-01-01T00:00:00Z"}
  },
  "retrievers": ["keyword", "dense"],
  "top_k": 50,
  "page_token": null
}
```

统一响应包含：

- `asset_id/segment_id`；
- 最终排序分；
- 关键词命中、向量相似度和融合排名；
- 模型和索引版本；
- 下一页 Token，而不是大 Offset 分页；
- 权限校验后的 OBS 访问地址或受控预签名链接。

---

## 5. 路线一：ClickHouse 26.3 LTS 统一检索

### 5.1 目标架构

```text
PostgreSQL + Outbox
        │
RabbitMQ/Celery ── Embedding Service
        │                 │
        └────────┬────────┘
                 ▼
        ClickHouse 26.3 LTS
  结构化列 + JSON + Text Index + Vector Index
                 │
            Search API
```

Milvus Lite 完全退出在线检索。ClickHouse 同时保存检索元数据、可检索文本和 CLIP 向量。

### 5.2 数据模型

建议从每 App 一表迁移为共享表，示意结构如下：

```sql
CREATE TABLE search_segments_v2
(
    app_id UUID,
    segment_id UUID,
    asset_id UUID,
    media_type LowCardinality(String),
    object_uri String,
    file_name String,
    searchable_text String,
    capture_time DateTime64(3),
    device_id String,
    h3_cell_r7 UInt64,
    h3_cell_r9 UInt64,
    permission_scope Array(String),
    metadata JSON,
    embedding Array(Float32),
    model_version LowCardinality(String),
    entity_version UInt64,
    is_deleted UInt8,
    updated_at DateTime64(3),

    INDEX idx_text searchable_text
        TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_embedding embedding
        TYPE vector_similarity('hnsw', 'cosineDistance', <DIMENSION>)
)
ENGINE = ReplacingMergeTree(entity_version)
PARTITION BY toYYYYMM(capture_time)
ORDER BY (app_id, media_type, capture_time, segment_id);
```

DDL 是设计示意，正式实施必须用实际 Chinese-CLIP 维度和 26.3 安装版本验证。分区默认只按月份，避免形成“App 数 × 月份数”的过量分区；`app_id`通过排序键和必要的 Shard Key 实现裁剪与路由。中文全文不应直接假设空格分词有效，需要对 `splitByNonAlpha`、N-Gram、预分词 Token 数组分别做质量和容量测试。

### 5.3 查询实现

#### 5.3.1 过滤 + 向量排序

```sql
SELECT segment_id, asset_id,
       cosineDistance(embedding, {query_vector}) AS distance
FROM search_segments_v2
WHERE app_id = {app_id}
  AND is_deleted = 0
  AND media_type IN {media_types}
  AND capture_time BETWEEN {start} AND {end}
ORDER BY distance
LIMIT {top_k};
```

这是本路线最成熟、最短的混合路径。

#### 5.3.2 关键词条件 + 向量排序

```sql
SELECT segment_id, asset_id,
       cosineDistance(embedding, {query_vector}) AS distance
FROM search_segments_v2
WHERE app_id = {app_id}
  AND is_deleted = 0
  AND hasAnyTokens(searchable_text, {query_text})
ORDER BY distance
LIMIT {top_k};
```

这里的关键词主要作为候选过滤条件，最终排名仍由向量距离决定。

#### 5.3.3 双路相关性融合

若要求关键词候选和向量候选分别排序，再做 RRF，需要额外建设：

1. 预分词列和词项统计；
2. 自定义关键词分数，或预计算 IDF/字段权重；
3. 关键词 Top-N 与向量 Top-N 两个子查询；
4. 使用窗口函数和 `FULL OUTER JOIN` 实现 RRF；
5. 对跨分片 Top-K、内存、执行时间和分页做专项测试。

这会削弱本路线“简单统一”的优势。因此路线一的默认范围应明确为：**Token/结构化过滤 + 稠密向量排序**。只有业务验收确实需要独立关键词相关性时，才增加自定义融合。

### 5.4 实施步骤

1. 盘点 24.3 中全部 App 表、字段类型、分区、TTL、索引和查询模板。
2. 备份并搭建独立 ClickHouse 26.3 LTS 环境，不原地跨大版本升级生产库。
3. 在 26.3 创建共享 `search_segments_v2` 和 `dataset_membership_v2`。
4. 将旧表按 App 批量导出为 Parquet，补充 `app_id/asset_id/segment_id` 后导入新表。
5. 导出 Milvus Lite 向量，校验维度、归一化和 IP/Cosine 等价性后导入向量列。
6. 物化全文与向量索引，完成冷/热查询和索引大小测试。
7. 新写入通过 Outbox 同步到 26.3；旧生产查询仍指向原系统。
8. 运行影子查询，对比过滤正确率、向量 Recall、排序和延迟。
9. 进入维护窗口：暂停摄入、排空 `serial_ch` 和索引队列、同步最后增量、执行数量与版本校验。
10. 切换 Search API 数据源，保留旧 ClickHouse 和 Milvus Lite 只读回滚窗口。

### 5.5 优点

- 检索层从 ClickHouse + 多 Milvus Lite 文件减少为一个 ClickHouse 集群。
- 保留现有 SQL、ClickHouse 运维经验、聚合、H3 和数据导入链路。
- 不需要跨数据库融合、双库对账和两套备份。
- ClickHouse 25.8 后向量索引已 GA，26.3 是 LTS，升级收益确定性较高。
- 全文、向量和分析可以访问同一行，权限、删除和版本传播更直接。

### 5.6 缺点与风险

- 26.3 全文索引不提供原生 BM25，难以直接实现高质量“关键词排名 + 向量排名”融合。
- HNSW 是当前主要 ANN 路线，向量量大、维度高时内存和索引构建压力需要实测。
- `ReplacingMergeTree` 合并前可能存在多版本，在线查询必须验证删除和更新的可见性。
- 全文与向量索引会增加写入、合并、存储和恢复成本；ClickHouse 官方全文基准曾显示索引可能显著降低写入吞吐。
- ClickHouse 是分析数据库，纯向量高并发下不一定优于专用向量引擎。

### 5.7 适用与否决条件

适用：

- 第一阶段接受“关键词/结构化条件过滤 + 向量排序”；
- 希望以最低迁移风险快速淘汰 Milvus Lite；
- 在线查询并发中等，分析和元数据过滤仍占主要比例。

否决条件：

- 必须具有可解释、可调优的 BM25 与向量双路融合；
- 1 亿向量下满足 Recall 门槛时 P95/P99 或内存不可接受；
- 持续写入和 Merge 导致索引新鲜度、召回或延迟持续不稳定。

---

## 6. 路线二：迁移到 Apache Doris 4.0

### 6.1 目标架构

```text
PostgreSQL + Outbox
        │
RabbitMQ/Celery ── Embedding Service
        │                 │
        └────────┬────────┘
                 ▼
          Apache Doris 4.0
   结构化 + VARIANT + 倒排/BM25 + ANN
                 │
            Search API
```

Doris 同时替代当前 ClickHouse 和 Milvus Lite，成为唯一检索引擎。OBS/MinIO 和 PostgreSQL 不变。

### 6.2 推荐生产拓扑

非 Kubernetes 环境下建议：

- PoC：1 FE + 1～3 BE；
- 首期生产：3 FE + 3 BE，前置负载均衡；
- FE、BE 使用独立持久卷和独立备份目录；
- 容器只承担进程包装，状态盘、主机时钟、文件句柄、网络和资源限制按数据库要求配置；
- 向量索引热数据内存与普通查询内存分开预留，禁止把全部内存分配给 BE 查询执行。

Doris 是一个数据库产品，但生产上包含 FE、BE 等多个运维单元，“单引擎”不等于“单进程”。

### 6.3 数据模型

建议使用 Unique Key Merge-on-Write 模型保存当前可检索状态：

```sql
CREATE TABLE search_segments_v2
(
    app_id VARCHAR(36) NOT NULL,
    segment_id VARCHAR(36) NOT NULL,
    asset_id VARCHAR(36) NOT NULL,
    media_type VARCHAR(32) NOT NULL,
    object_uri STRING NOT NULL,
    file_name STRING NOT NULL,
    searchable_text STRING NOT NULL,
    capture_time DATETIMEV2(3),
    device_id STRING,
    h3_cell_r7 BIGINT,
    h3_cell_r9 BIGINT,
    permission_scope ARRAY<STRING>,
    metadata VARIANT,
    embedding ARRAY<FLOAT> NOT NULL,
    model_version VARCHAR(128) NOT NULL,
    entity_version BIGINT NOT NULL,
    is_deleted BOOLEAN NOT NULL,
    updated_at DATETIMEV2(3),

    INDEX idx_text(searchable_text) USING INVERTED
        PROPERTIES("parser" = "unicode", "support_phrase" = "true"),
    INDEX idx_embedding(embedding) USING ANN
        PROPERTIES(
            "index_type" = "hnsw",
            "metric_type" = "inner_product",
            "dim" = "<DIMENSION>"
        )
)
UNIQUE KEY(app_id, segment_id)
DISTRIBUTED BY HASH(app_id, segment_id) BUCKETS AUTO
PROPERTIES("enable_unique_key_merge_on_write" = "true");
```

正式 DDL 必须以安装版本语法和模型限制为准。Doris ANN 当前使用 `ARRAY<FLOAT> NOT NULL` 和固定维度。Chinese-CLIP 若使用 Cosine，相应向量应先单位归一化，再使用 Inner Product，并通过样本对比确认排序与现系统一致。

当前 Doris 官方资料未显示与 ClickHouse 等价的原生 H3 函数链，因此应继续在摄入阶段预计算多个分辨率的 `h3_cell`，用普通列和倒排索引执行过滤与聚合；复杂空间计算再使用 Doris GIS 函数或应用服务。

### 6.4 两种混合查询方式

#### 6.4.1 预过滤型混合查询

```sql
SELECT segment_id, asset_id,
       inner_product_approximate(embedding, ?) AS vector_score
FROM search_segments_v2
WHERE app_id = ?
  AND is_deleted = false
  AND searchable_text MATCH_ANY ?
  AND capture_time BETWEEN ? AND ?
ORDER BY vector_score DESC
LIMIT ?;
```

倒排索引和结构化条件先过滤，再由 ANN 排序。执行链短，适合用户明确要求关键词必须命中的场景。

#### 6.4.2 BM25 + ANN + RRF

```sql
WITH keyword_result AS (
    SELECT segment_id,
           ROW_NUMBER() OVER (ORDER BY score() DESC) AS keyword_rank
    FROM search_segments_v2
    WHERE app_id = ?
      AND is_deleted = false
      AND searchable_text MATCH_ANY ?
    ORDER BY score() DESC
    LIMIT 100
),
vector_result AS (
    SELECT segment_id,
           ROW_NUMBER() OVER (
               ORDER BY inner_product_approximate(embedding, ?) DESC
           ) AS vector_rank
    FROM search_segments_v2
    WHERE app_id = ? AND is_deleted = false
    ORDER BY inner_product_approximate(embedding, ?) DESC
    LIMIT 100
)
SELECT COALESCE(k.segment_id, v.segment_id) AS segment_id,
       COALESCE(1.0 / (60 + k.keyword_rank), 0) +
       COALESCE(1.0 / (60 + v.vector_rank), 0) AS rrf_score
FROM keyword_result k
FULL OUTER JOIN vector_result v
  ON k.segment_id = v.segment_id
ORDER BY rrf_score DESC
LIMIT 50;
```

Doris 没有内置 `rrf()` 函数，RRF 是 SQL 模式。必须实测两个 Top-N 子查询、窗口函数和 Join 在分布式环境的执行计划及尾延迟。

### 6.5 索引策略

- HNSW：用于热点、低延迟、高召回数据；需要较多内存，冷启动需要预热。
- IVF：用于更大规模数据，索引构建较快，但需要调 `nlist/nprobe`。
- IVF On-Disk：作为 1 亿级或内存受限候选，必须单独测试冷数据 P95/P99。
- 倒排索引：用于 App、设备、状态、时间、数组和全文；中文语料比较 `chinese/unicode/icu` 或自定义 Analyzer。
- ANN 可同步随 Segment 构建，也可批量导入后异步 Build。历史迁移优先“先导数据、Full Compaction、再 Build Index”，避免重复构建。

### 6.6 实施步骤

1. 建立 Doris 4.0 独立 PoC 集群，首先验证表模型、中文 Analyzer、H3 替代和权限条件。
2. 从 ClickHouse 24.3 导出统一 Parquet，并将 Milvus Lite 向量合并进同一 `segment_id` 行。
3. 使用 Stream Load 或批量导入写入 Doris；历史回填期间关闭同步 ANN 构建。
4. 执行 Compaction，随后分别建立倒排和 ANN 索引并预热。
5. 建立 Outbox 消费者，验证秒级 Upsert、Delete、ACL 变更和重复事件。
6. 实现预过滤型查询和 SQL RRF 查询两套模板。
7. 运行 10M、50M、100M 三档容量测试；100M 可以使用扩展数据，但字段、向量分布和过滤选择率必须接近生产。
8. 运行持续写入 + Compaction + 查询 72 小时稳定性测试。
9. 通过决策门后，进行第二次全量生产数据迁移和增量追平。
10. 维护窗口暂停写入、排空队列、同步最后增量，校验数量、版本和抽样内容后切换 DSN。

### 6.7 优点

- 一个数据库同时处理结构化、全文、BM25、向量和分析。
- 能在同一行上完成权限、版本、全文和向量计算，减少跨库数据漂移。
- Doris 4.0 已提供 HNSW、IVF、IVF On-Disk，容量策略比单一 HNSW 更灵活。
- Unique Key Merge-on-Write 更适合当前状态型检索记录的 Upsert。
- SQL RRF 比应用层自行拼接更集中，结果解释和审计更容易。
- `VARIANT`可承载动态元数据，适合从现有 `meta JSON` 迁移。

### 6.8 缺点与风险

- 必须迁移全部 ClickHouse 表、查询、导入任务、聚合、H3 和运维体系。
- Doris ANN 是 4.0 新增能力，生产案例和长期稳定性不如成熟 OLAP 功能。
- ANN 索引按 Segment 构建，Compaction 会影响索引构建、召回和资源使用。
- HNSW 冷加载会显著影响延迟，需要预热和容量隔离。
- RRF 不是内置算子，而是两个候选查询加 Join；高并发下可能成为热点。
- 团队需要学习 FE/BE、Tablet、Compaction、Bucket、Unique Key 和索引调优。
- 备份恢复、索引重建及不同部署模式的限制必须以实际 4.0 小版本验证。

### 6.9 适用与否决条件

适用：

- 关键词相关性和向量相关性都重要；
- 希望长期维持一个检索数据副本；
- 接受一次较大的迁移，换取更清晰的数据和查询模型。

否决条件：

- 持续写入/Compaction 期间 Recall、P99 或索引新鲜度无法达标；
- 100M 规模下 RRF SQL 的延迟、内存或并发不可接受；
- 迁移后 H3、聚合、JSON 或关键 ClickHouse 查询无法等价实现；
- 冷启动或故障恢复时间超过生产容忍度。

---

## 7. 路线三：ClickHouse 26.3 LTS + Qdrant

### 7.1 目标架构

```text
                    PostgreSQL + Outbox
                           │
                    RabbitMQ/Celery
                      ┌────┴────┐
                      │         │
              ClickHouse     Qdrant
              元数据/全文    Dense/Sparse/Payload
                      │         │
                      └────┬────┘
                           │
                     Search Gateway
               召回、RRF、ACL、去重、补充字段
```

Qdrant 取代多个 Milvus Lite 文件。ClickHouse 升级并重构共享表，但不再承担主向量索引。

### 7.2 数据职责

#### ClickHouse

- 结构化元数据、动态 JSON、高频业务字段；
- 全文 Token 索引；
- DatasetVersion 成员投影；
- H3 与聚合；
- 搜索结果的最终权限验证和字段补充；
- 检索日志、质量评估和运营分析。

#### Qdrant

每个 Point 使用 `segment_id`作为 UUID：

```text
collection: multimodal_segments_<model_family>

point_id: segment_id
vectors:
  image_dense: <DIMENSION, Cosine>
  text_dense:  <DIMENSION, Cosine>       # 可按模型决定是否与image共用
  text_sparse: <sparse>                  # 可选的第二阶段能力
payload:
  app_id
  asset_id
  media_type
  capture_time
  device_id
  permission_scope
  model_version
  is_deleted
```

只复制向量检索真正需要的过滤字段。大 JSON、完整 DatasetVersion 清单和分析字段不应全部复制到 Qdrant。

Collection 按模型维度和距离度量划分，不按 App、目录或 DatasetVersion 创建。多租户通过 `app_id` Payload、Payload 索引和必要时的自定义 Sharding 隔离。

### 7.3 两种查询实现

#### 7.3.1 第一阶段：ClickHouse 关键词 + Qdrant Dense

1. Search Gateway 调用 Embedding Service 得到 Query Vector。
2. ClickHouse 执行结构化/全文查询，产生关键词候选 Top-N。
3. Qdrant 使用 `app_id/ACL/media/time` Payload 预过滤并产生向量候选 Top-N。
4. Gateway 按 `segment_id`进行加权 RRF。
5. 候选回到 ClickHouse 做最终权限验证、删除校验和字段补充。
6. 资产级去重后返回游标分页结果。

此方式不强制稀疏向量，但 ClickHouse 关键词候选缺少原生 BM25，关键词排名需要自定义匹配分或只按业务字段权重排序。

#### 7.3.2 推荐演进：Qdrant Dense + Sparse 统一召回

1. 为 `searchable_text`生成中文 BM25/SPLADE 等稀疏向量。
2. Qdrant 使用 Dense 与 Sparse 两路 Prefetch。
3. 在顶层 Query 使用 RRF 或 DBSF 做全局融合。
4. Payload 完成高频结构化和 ACL 预过滤。
5. ClickHouse 只做最终一致性校验、复杂 DatasetVersion 过滤和字段补充。

这个方案的混合检索质量最高、查询职责最清晰，但增加稀疏向量生成、词表/模型版本和重建管理。它是演进项，不是第一阶段硬门槛。

### 7.4 Search Gateway 设计要点

- 使用 RRF 合并排名，不直接相加未经校准的关键词分和向量距离。
- 候选池默认从每路 `5 × top_k`开始，根据离线 NDCG 调整到 3～20 倍。
- 所有候选携带 `index_version`和`model_version`，禁止混合不同向量空间。
- 深分页使用签名 Page Token，保存每路游标、融合版本和最后排序键。
- Qdrant 的 Payload ACL 用于预过滤，ClickHouse/PostgreSQL 权限用于最终校验。
- 对巨型 DatasetVersion，不发送百万 ID；采用版本标签物化、分片路由或 Qdrant 召回后 ClickHouse 过滤并动态超采样。
- 多分片 Qdrant 中，Fusion 必须置于顶层 Query 才具有跨分片全局融合语义。

### 7.5 一致性与重建

Outbox 的两个消费者分别更新 ClickHouse 和 Qdrant，并在 PostgreSQL 保存：

```text
source_version
clickhouse_indexed_version
qdrant_indexed_version
clickhouse_status
qdrant_status
```

Search Gateway 默认只返回两个目标版本都不落后于要求版本的结果；若业务允许，可以配置“向量索引稍后可见”，但删除和 ACL 变更必须优先传播。

Embedding 的事实快照建议保存到 OBS/MinIO Parquet：

```text
segment_id, vector, model_version, dimension,
normalized, payload_hash, generated_at
```

Qdrant 被视为可重建索引，而不是向量唯一事实来源。

### 7.6 部署建议

- PoC/较小生产：Qdrant 单节点 + 高性能本地 NVMe + 定期远端 Snapshot。
- 高可用：3 个 Qdrant 节点、多个 Shard、复制因子 2，前置负载均衡。
- ClickHouse 根据现网容量采用 1 副本起步或 2 副本 + Keeper。
- 非 Kubernetes 环境下使用固定主机名、显式端口、独立数据卷、进程守护和监控。
- Qdrant OSS 扩容后已有 Shard 不会自动完成所有重新均衡工作，需要把 Shard/Replica 移动写入运维手册。

### 7.7 实施步骤

1. 先完成 ClickHouse 26.3 升级、共享表和 Outbox，不立即切换向量查询。
2. 搭建 Qdrant，按实际向量维度创建 Collection 和 Payload Index。
3. 将 Milvus Lite 数据统一导出为 Parquet，进行重复、孤立、删除和归一化校验。
4. 批量导入 Qdrant，禁止继续按项目创建本地 DB。
5. 建立双消费者、索引状态、重试、死信、对账和重建工具。
6. 开发 Search Gateway，先实现 Filter + Dense，再实现关键词与 Dense RRF。
7. 运行影子查询，比较旧 Milvus、Qdrant Dense 和新混合结果。
8. 维护窗口暂停摄入、追平两库版本、生成 Snapshot、执行一致性校验后切换。
9. 稳定后评估 Sparse/BM25 向量和 Qdrant 内部融合。

在第 3～7 步之间应使用同一份集中式数据额外部署 Milvus Standalone（或与目标规模匹配的 Milvus 集群）作为对照。若 Qdrant 在检索质量、过滤性能、资源、非 K8s 运维或模型切换方面没有形成足以覆盖迁移成本的优势，则停止 Qdrant 切换，不因为已经完成 PoC 而强行上线。

### 7.8 优点

- Qdrant 专门针对向量、Payload 过滤、量化、分片和在线 Upsert 设计。
- 通过一个集中 Collection 消除当前多 Milvus Lite 文件和 Python 全局合并。
- Dense/Sparse、RRF/DBSF 和多阶段查询为后续混合检索提供清晰演进路径。
- ClickHouse 继续发挥现有聚合、H3、动态元数据和全文能力，不需要整体迁移。
- 向量模型升级可以通过新 Collection + Alias 实现蓝绿切换。

### 7.9 缺点与风险

- 保留两个检索引擎、两套索引、两套容量和备份。
- 需要 Outbox、双消费者、对账、补偿和重建，数据管理复杂度高于单引擎。
- 跨库候选合并会增加网络、尾延迟和分页复杂度。
- ClickHouse 与 Qdrant 的过滤表达能力不同，必须维护统一过滤 DSL 和两个编译器。
- Qdrant 自托管分布式需要手工规划 Shard、Replica、负载均衡和扩容。
- 最终权限校验后可能导致结果不足，需要动态超采样和补召回。

### 7.10 适用与否决条件

适用：

- 向量召回质量、低延迟或模型演进优先级高于组件数量；
- 单引擎路线无法通过 100M 向量或持续写入测试；
- 接受建设独立 Search Gateway 和索引一致性平台。

否决条件：

- 团队无法承担两个状态数据库的备份、监控、恢复和对账；
- 业务要求所有混合搜索严格通过单条 SQL 完成；
- 双库索引版本差、权限传播或删除传播无法稳定达到秒级目标。

---

## 8. 三路线对比与决策矩阵

### 8.1 详细对比

| 维度 | ClickHouse 26.3 | Apache Doris 4.0 | ClickHouse + Qdrant |
|---|---|---|---|
| 结构化过滤 | 强 | 强 | 强，需维护双侧过滤子集 |
| 关键词/全文 | Token 全文索引，偏过滤 | 倒排 + BM25 | CH Token；演进后可用 Qdrant Sparse |
| CLIP ANN | HNSW，已 GA | HNSW/IVF/IVF On-Disk，4.0 新增 | Qdrant HNSW/量化/On-Disk |
| 原生关键词相关性 | 弱，无原生 BM25 | 强，`score()` BM25 | 第一阶段弱；Sparse 路线强 |
| 融合 | 自定义 SQL/业务逻辑 | SQL RRF，无内置函数 | Gateway RRF 或 Qdrant RRF/DBSF |
| 单引擎 | 是 | 是 | 否 |
| 当前技能复用 | 最高 | 中 | 较高，新增 Qdrant/Gateway |
| 迁移量 | 中 | 最大 | 中到大 |
| 数据一致性 | 最简单 | 最简单 | 最复杂 |
| H3 继承 | 最好 | 需预计算/改写 | ClickHouse 保留 |
| 动态 JSON | ClickHouse JSON | Doris VARIANT | CH JSON + 少量 Payload |
| 向量性能上限 | 需实测 | 需实测，算法选择更多 | 通常最高，但以项目 PoC 为准 |
| 组件减少 | 最好 | 好，但 FE/BE 进程较多 | 最差 |
| 最大风险 | 混合相关性不完整 | 新 ANN 的生产成熟度 | 双库治理与跨库融合 |

### 8.2 相较现有设计的收益与新增成本

#### 8.2.1 现状不是“零成本基线”

当前方案已经部署了 ClickHouse、Milvus 相关代码和 CLIP 服务，但继续保持不变仍然存在长期成本：

- 每个 App 独立 ClickHouse 表带来的动态 DDL、动态 SQL 和跨 App 查询成本；
- 多个 Milvus Lite `.db`文件的发现、加载、连接、逐库查询和生命周期管理；
- Python 并发查询多个 DB 后合并排序，无法自然保证全局 ANN Top-K；
- ClickHouse、Milvus Lite、PostgreSQL 和 OBS 之间缺少统一 Watermark、对账和重建；
- 文件路径承担身份，移动、重命名、版本复用和删除容易产生错误；
- 结构化查询与向量查询并存，但没有统一的关键词、向量和业务条件融合排序；
- DatasetVersion、Manifest、成员关系和字段治理问题仍需人工或业务代码补偿。

因此应比较的是“新路线成本”与“继续维护现状的隐性成本和能力缺口”，不能把现有软件已经安装等同于继续维护没有成本。

#### 8.2.2 总体净变化

| 路线 | 相较现状直接解决 | 仍需共同重构解决 | 新增一次性成本 | 新增长期成本 |
|---|---|---|---|---|
| ClickHouse 26.3 统一检索 | 淘汰多 Milvus Lite；共享表支持跨 App；同引擎过滤/全文/向量；减少检索组件 | DatasetVersion、Manifest、字段治理、内容解析、稳定 ID | ClickHouse 跨版本迁移；全部向量回填；共享表改造；全文/向量索引重建；查询 API 改写 | ClickHouse 承担向量内存、索引构建、Merge 和恢复；分析与在线向量负载互相影响 |
| Apache Doris 4.0 | 在一个引擎中统一结构化、BM25、ANN 和分析；淘汰旧 CH 表与 Milvus Lite | DatasetVersion、Manifest、字段治理、内容解析、稳定 ID | 全量数据库迁移；SQL/DDL/导入/H3 改写；FE/BE 部署；团队学习；历史索引重建 | FE/BE 集群运维；Compaction 与 ANN 重建；索引预热；新 ANN 能力的升级与故障风险 |
| ClickHouse + Qdrant | 集中向量服务；消除多 Lite 文件；保留 CH/H3；向量过滤、模型切换和融合能力增强 | DatasetVersion、Manifest、字段治理、内容解析、稳定 ID，同时还需双库一致性 | CH 升级；Qdrant 部署；向量迁移；Search Gateway；过滤 DSL 双编译器；双库影子验证 | 两套数据库容量、监控、备份、恢复和升级；Payload 重复；跨库延迟、补召回和对账 |

#### 8.2.3 ClickHouse 26.3 统一检索相较现状

**能够消除或改善：**

- 将“ClickHouse + 多个本地向量 DB + Python 合并”收敛为一个检索引擎；
- 共享表替代每 App 一表，跨 App 检索不再逐表拼接；
- 结构化条件、Token 全文条件和向量排序可以在同一 SQL、同一权限范围内执行；
- 删除、ACL 和 DatasetVersion 高频标签不再需要同时维护两套检索存储；
- 现有 ClickHouse SQL、H3、聚合和运维经验大部分可以复用。

**不能单独解决：**

- 不可变 DatasetVersion、Manifest、血缘、成员复用和字段冲突；
- 高质量 BM25 与 Dense 双路相关性融合；
- OCR、ASR、Caption 和 Embedding 的生成质量；
- 单一数据库本身不能消除错误 Schema、错误权限和错误业务状态。

**新增成本：**

- ClickHouse 从分析与元数据查询引擎变成在线向量服务，CPU、RAM、NVMe 和索引重建资源需要重新核算；
- 全文和 HNSW 索引会增加磁盘、写放大、Merge 压力和备份恢复时间；
- 向量全部导入 ClickHouse 后，分析查询、数据导入和在线向量查询可能争用资源；
- 24.3 到 26.3 不应直接覆盖升级，需要并行新集群、双份迁移空间和回滚窗口。

**领导决策口径：**

> 选择这条路线不是因为 ClickHouse 能提供最强向量检索，而是因为它用最小的技术变化消除当前最不易维护的多 Milvus Lite 文件，并把第一阶段要求的过滤、关键词匹配和 CLIP 向量排序收敛到一套数据。如果业务必须具备 BM25 与 Dense 双路融合，或单引擎向量性能不能达标，就不应选择它。

#### 8.2.4 Apache Doris 相较现状

**能够消除或改善：**

- 彻底替换每 App ClickHouse 表和多个 Milvus Lite DB，形成一个共享检索数据模型；
- 结构化索引、倒排全文、BM25、ANN 和 RRF SQL 可以围绕同一行和同一引擎执行；
- 不再需要跨数据库同步关键词字段、向量和过滤条件；
- Unique Key Merge-on-Write 更贴近当前检索状态记录的 Upsert 需求；
- HNSW、IVF 和 IVF On-Disk 为不同规模与内存条件提供更多选项。

**不能单独解决：**

- DatasetVersion 事实、Manifest、字段注册和版本血缘仍然应放在共同控制面；
- 多模态原始内容仍然保存在 OBS/MinIO，Doris 不会自动生成 Caption、OCR、ASR 和向量；
- 单引擎不会自动带来正确的 RRF 参数、中文分词和权限模型。

**新增成本：**

- 所有 ClickHouse DDL、SQL、导入任务、备份、监控、H3 查询和故障手册都要迁移或重写；
- 生产上需要维护 FE、BE、负载均衡、持久卷和 Compaction，不是安装一个进程即可；
- ANN 是 Doris 4.0 新能力，持续写入、Compaction、冷加载、索引重建和小版本升级存在额外验证成本；
- 团队已有 ClickHouse 经验只能部分复用，排障知识需要重新积累；
- 迁移窗口内需要同时保留原 ClickHouse/Milvus 和 Doris，短期硬件占用最高。

**领导决策口径：**

> 迁移 Doris 的理由不是“换一个与 ClickHouse 类似的数据库”，而是为了在减少检索产品的同时获得 ClickHouse 26.3 当前缺少的原生 BM25 和更完整的 ANN 选择。只有这些收益能在 Camera DMP 数据上通过 PoC，并且足以抵消全量迁移和新功能成熟度风险时，迁移才成立。

#### 8.2.5 ClickHouse + Qdrant 相较现状

**能够消除或改善：**

- 把大量本地 Milvus Lite 文件收敛为一个有明确 Collection、Shard、Replica、Snapshot 和 Payload Index 的集中服务；
- 向量过滤、在线 Upsert、模型版本、Collection Alias、量化和多阶段检索更容易统一管理；
- 可以逐步从 Dense 检索演进到 Dense + Sparse，并在 Qdrant 内使用 RRF/DBSF；
- ClickHouse 继续承担现有元数据、H3、分析和全文职责，不需要整体迁移到新 OLAP 引擎；
- 单独扩容向量层，不让向量负载直接影响 ClickHouse 分析查询。

**不能单独解决：**

- 组件数量不会减少为一个检索引擎；
- DatasetVersion、Manifest、字段治理和多模态解析仍需共同控制面；
- ClickHouse 与 Qdrant 的版本差、删除、ACL 和分页需要额外治理；
- Qdrant 不会因为替换了 Milvus 就自动获得更高 Recall 或更低 P99，必须实测。

**新增成本：**

- 新增 Qdrant 集群、持久盘、备份、监控、升级和故障恢复；
- 重写 Milvus SDK 调用和当前多目录查询逻辑；
- 建设 Search Gateway、候选超采样、RRF、最终权限校验和稳定分页；
- ClickHouse 与 Qdrant 重复保存部分 ID、ACL、版本和过滤 Payload；
- 两个消费者、两个索引 Watermark、双库对账和故障补偿成为长期平台能力；
- 分布式 Qdrant 的 Shard/Replica 规划和自托管再均衡需要人工运维。

**领导决策口径：**

> 选择 Qdrant 的目的不是把一个可用的集中式 Milvus 无理由换掉；当前实际问题是使用了大量 Milvus Lite 本地文件，向量检索本来就需要重构为集中式服务。Qdrant 只在非 Kubernetes 运维、Payload 过滤、Dense/Sparse 融合、模型蓝绿和实际性能上优于“集中式 Milvus 对照组”时才值得采用。

#### 8.2.6 已有 Milvus，为什么还要评估 Qdrant

首先要区分两件事：

- **当前实现**：`MilvusClient(uri="...db")`连接多个本地 `.db`，属于多个 Milvus Lite 实例，由应用代码逐个查询和合并。
- **可选对照**：部署一个 Milvus Standalone 或 Milvus Distributed，把数据迁入共享 Collection，由服务端完成检索和全局 Top-K。

所以“继续用Milvus”也不是保持现状不动，而是一次集中化迁移。Qdrant与集中式Milvus应按下表比较：

| 决策维度 | 集中式 Milvus | Qdrant |
|---|---|---|
| 现有 SDK 复用 | 更高，仍需删除文件扫描和多库合并逻辑 | 需要更换客户端和接口 |
| 非 K8s 起步 | Standalone 可用；Distributed 更重 | 单节点/少节点起步相对直接 |
| Dense CLIP 检索 | 成熟 | 成熟 |
| 标量/Payload过滤 | 支持，需要按真实过滤模型验证 | Payload 与 Payload Index 是核心能力 |
| Dense + Sparse 混合 | 支持 | 支持，Query API 提供 RRF/DBSF 和多阶段查询 |
| 超大规模分布式 | Milvus Distributed 优势明显，但组件更多 | 支持分片复制，自托管扩容与再均衡需规划 |
| 迁移工作 | 数据必须从 Lite 文件汇总导入 | 数据必须从 Lite 文件导出并转换导入 |
| 团队学习 | pymilvus经验可部分复用 | 需要学习 Qdrant Collection、Payload、Shard 与 Query API |

Qdrant PoC必须加入集中式Milvus作为向量层对照组，并使用同一批数据、同一向量、同一过滤条件、同一硬件级别比较：

- Recall@K、NDCG、P95/P99 和 QPS；
- 100%、10%、1%、0.1%、0.01%过滤选择率；
- 持续 Upsert/Delete 与索引构建；
- 冷启动、备份恢复和模型蓝绿；
- 单节点与目标生产拓扑下的资源占用；
- SDK改造、监控、备份和故障恢复工作量。

如果业务只需要 Dense CLIP、现有团队熟悉 Milvus，且集中式 Milvus 达到全部 SLA，就没有充分理由仅为技术偏好替换为 Qdrant。此时应保留 ClickHouse + 集中式 Milvus，或重新评估是否仍需要路线三。

#### 8.2.7 成本口径

本方案不在缺少硬件和人员数据时虚构金额。PoC和实施评审必须分别提交以下成本：

| 成本类型 | 必须计入的内容 |
|---|---|
| 一次性研发 | 数据模型、API、Indexer、查询改写、迁移程序、兼容与回滚 |
| 一次性基础设施 | 新旧系统并行期的服务器、磁盘、网络、Snapshot和临时重建空间 |
| 数据迁移 | 全量导出、Hash/ID转换、向量重建或搬迁、索引Build和最终追平 |
| 学习与验证 | Doris/Qdrant/Milvus集群知识、PoC、压测、故障演练和Runbook |
| 长期计算存储 | 数据、全文索引、ANN索引、副本、备份、重建临时空间和网络 |
| 长期运维 | 升级、安全补丁、监控、扩容、Compaction/Merge、Shard和备份恢复 |
| 故障复杂度 | 单库故障、双库漂移、权限延迟、索引损坏、模型回滚和人工排查 |
| 机会成本 | 路线建设期间无法投入的业务需求，以及选错后再次迁移的成本 |

以下成本是三条路线都必须承担的共同改造，不应重复算到某一候选头上：稳定 ID、DatasetVersion 控制面、Manifest、字段注册、Outbox、Embedding Service 解耦、统一 Search API、历史数据清洗和业务验收集。路线成本比较只计算在这些共同基础之上的增量。

领导评审时应同时展示“新路线新增成本”和“现状每年继续付出的维护/缺陷成本”，再判断净收益。

### 8.3 按当前优先级的评分

评分只用于表达决策逻辑，不代替 PoC。权重按已确认目标设置：

- 混合检索完整度：45%
- 组件与数据管理：25%
- 迁移风险：10%
- 千万至一亿性能潜力：10%
- 团队运维门槛：10%

| 路线 | 混合检索 | 组件/管理 | 迁移风险 | 性能潜力 | 运维门槛 | 加权参考 |
|---|---:|---:|---:|---:|---:|---:|
| ClickHouse 26.3 | 3.0 | 5.0 | 4.7 | 3.5 | 4.7 | 约 78/100 |
| Apache Doris 4.0 | 4.5 | 4.2 | 2.8 | 4.0 | 3.0 | 约 81/100 |
| ClickHouse + Qdrant | 4.8 | 2.5 | 3.7 | 4.8 | 3.0 | 约 79/100 |

因此 Doris 获得第一候选，但领先幅度不大，且它的得分依赖 ANN PoC 通过。如果把迁移风险或团队熟悉度调高，ClickHouse 26.3 会成为第一名；如果把向量性能和检索质量调高，ClickHouse + Qdrant 会成为第一名。

### 8.4 决策树

```text
是否需要独立BM25排名与Dense排名融合？
├─ 否：优先 ClickHouse 26.3 统一检索
└─ 是
   ├─ Doris 4.0 在100M、持续写入、Compaction下通过PoC？
   │  ├─ 是：选择 Doris 4.0
   │  └─ 否
   │     ├─ 可以接受双检索引擎？
   │     │  ├─ 是：对比集中式Milvus与Qdrant
   │     │  │       ├─ Qdrant有显著净收益：选择ClickHouse + Qdrant
   │     │  │       └─ 无显著净收益：保留集中式Milvus，不执行Qdrant路线
   │     │  └─ 否：退回 ClickHouse，降低关键词融合目标
   │     └─
   └─
```

### 8.5 常见决策问题与答复

#### 为什么不维持现在的 ClickHouse + Milvus Lite

因为当前并不是一个集中式 Milvus 服务，而是多个本地 DB 文件加应用层扇出查询。它可以满足有限规模的相似度搜索，但无法自然解决全局 Top-K、跨 App、统一 ACL、备份扩容、混合排序和索引对账。继续维持现状仍然需要重写身份、版本、事件和搜索网关，差别只是是否顺便把向量层集中化。

#### 为什么不只升级 ClickHouse，直接结束选型

如果业务接受关键词作为过滤条件、向量作为主要排序，ClickHouse 26.3 确实是最优先的低风险方案。如果要求独立 BM25 与 Dense 候选融合，或 ClickHouse 在 100M 向量下的 Recall/P99、内存和持续写入不能达标，才需要 Doris 或 Qdrant。

#### Doris也是数据库，为什么迁移后会更简单

简单的是数据和查询职责：结构化、全文、BM25、向量和分析位于同一引擎，不再跨库对账。但运维进程并不一定更少，Doris 生产部署仍有 FE、BE、负载均衡、Compaction 和索引预热。是否整体更简单必须把应用胶水减少与数据库运维增加同时计算。

#### Qdrant会增加组件，为什么还列为候选

因为“减少组件”是第二目标，混合检索质量是第一目标。如果两个单引擎方案不能达到向量 Recall、过滤后 Top-K 或尾延迟要求，专用向量引擎是合理代价。Qdrant 路线必须用性能和检索质量证明这项额外成本，而不是凭功能列表获选。

#### 做完共同重构后，是否可以不换数据库

可以。稳定 ID、Manifest、Outbox、集中 Embedding 快照和统一 API 完成后，如果集中式 Milvus 加现有 ClickHouse 已经达到目标，数据库迁移可以停止。共同重构的价值之一，就是降低选型绑定，让检索引擎能够被验证、替换和重建。

#### 单引擎是否意味着不再有数据一致性问题

不意味着。PostgreSQL、OBS/MinIO、检索引擎和Embedding Pipeline仍然是多个事实与投影环节。单引擎只能减少“两个检索库之间”的一致性问题，不能取消Manifest、Outbox、Watermark、对账和重建。

---

## 9. PoC 设计与决策门

### 9.1 数据集

必须使用 Camera DMP 自有数据，不用公开英文向量基准代替：

- 10M：快速调参与功能验证；
- 50M：验证容量变化和分片；
- 100M：最终决策规模；不足部分可以复制结构分布，但向量不得简单重复；
- 覆盖图片、视频关键帧、音频片段、文档和动态元数据；
- 保持真实 App、时间、设备、DatasetVersion 和 ACL 分布；
- 包含跨版本复用、内容修订、多父版本合并和字段冲突样本，而不是把每个版本简单复制成独立数据。

### 9.2 查询集

人工建立不少于 500 条查询，包含：

- 精确文件名、路径片段、设备编号；
- 中文关键词、中英混合和专有名词；
- 文本搜图、以图搜图；
- 关键词 + 语义互补查询；
- App、媒体类型、时间、地理、质量和 DatasetVersion 过滤；
- 父版本、子版本、合并版本和跨 App 授权检索；
- 同一 Asset 在多个版本复用、不同 Revision 以及版本回滚后的结果；
- 同名异义、类型变化、单位变化和不同来源优先级字段；
- 无结果和低相关结果；
- 权限隔离与权限刚撤销；
- 热门、高频词和稀有词；
- 模型版本切换和删除数据。

### 9.3 过滤选择率矩阵

每条路线都测试：

| 过滤后保留比例 | 目的 |
|---:|---|
| 100% | 纯向量性能与全局 Top-K |
| 10% | 普通 App/媒体过滤 |
| 1% | 时间、设备和质量组合 |
| 0.1% | 高选择性 DatasetVersion/ACL |
| 0.01% | 极高选择性和无结果边界 |

预过滤、后过滤、自动策略和超采样在不同选择率下表现可能相反，必须分别记录 Recall 与 P99。

### 9.4 运行状态

- 冷启动：数据库或节点重启后的首次查询；
- 热查询：索引和数据稳定缓存；
- 持续写入：同时执行 Upsert、Delete、Embedding 更新；
- 索引构建：历史 Build/Materialize 期间查询；
- Compaction/Merge：后台合并期间查询；
- 故障状态：单节点停止、磁盘压力、消息重复和网络超时；
- 恢复状态：从 Snapshot/Backup 恢复后首次服务。
- 向量引擎对照：Qdrant 路线使用集中式 Milvus 在同一数据、查询、过滤和硬件级别下进行对照测试。

### 9.5 决策门

#### Gate A：功能正确性

- 三路查询均可执行；
- 过滤和 ACL 100% 正确；
- 删除、移动、重命名不再依赖路径身份；
- 模型版本隔离正确；
- 已发布 DatasetVersion 不可原地修改，Manifest、成员数和 Checksum 一致；
- 多级数据集、版本 DAG 和指定版本成员解析正确；
- 同一内容跨版本复用时不产生非必要物理副本；
- 字段冲突按 `field_definition.merge_policy`处理，不发生静默覆盖；
- 导入任务失败后可以从 Chunk Checkpoint 幂等续写；
- 查询结果可解释。

任一失败，不进入性能测试。

#### Gate B：检索质量

- 达到 Recall@K 门槛；
- 混合 NDCG 不低于两种单路；
- 中文、路径和专有名词查询可接受；
- 过滤后 Top-K 不因错误的 Post-filter 明显缺失。

#### Gate C：性能与容量

- 100M 规模达到 P95/P99 门槛；
- 索引内存、磁盘和临时重建空间在硬件边界内；
- 持续写入不造成不可接受的查询抖动。
- 若选择 Qdrant，其相对集中式 Milvus 的质量、性能或长期运维收益足以覆盖 SDK、迁移和双库治理成本；否则 Qdrant 路线不通过。

#### Gate D：运维与恢复

- 完成备份、恢复和索引重建演练；
- 完成单节点故障演练；
- 监控能够发现索引落后、Recall 异常和数据漂移；
- 团队能够依据 Runbook 完成扩容、重建和回滚。

只有四个 Gate 全部通过，才允许进入生产迁移。

---

## 10. 统一迁移、停机切换与回滚

### 10.1 统一中间格式

所有旧 ClickHouse 表和 Milvus Lite DB 先转换成 Parquet 快照：

```text
snapshot_manifest
  snapshot_id
  created_at
  source_table_or_db
  row_count
  min/max entity_version
  schema_hash
  model_version
  file_list
  checksum

datasets/*.parquet
dataset_versions/*.parquet
dataset_version_parents/*.parquet
assets_and_revisions/*.parquet
search_documents/*.parquet
embeddings/*.parquet
dataset_membership/*.parquet
field_definitions/*.parquet
version_manifests/*.json|parquet
```

这样三条路线可以复用同一份迁移输入和验收脚本，也为将来换引擎保留退出路径。

### 10.2 历史迁移

1. 冻结旧 Schema，停止新增 Milvus Lite DB。
2. 盘点现有 Dataset、DatasetVersion、路径层级、版本成员和字段定义，生成旧标识到新稳定 ID 的映射。
3. 建立旧路径到 `asset_id/asset_revision_id/segment_id` 的映射，并通过内容 Hash 识别跨版本复用与真实内容修订。
4. 将路径层级转换为 `dataset.parent_dataset_id`，将已有派生关系转换为 `dataset_version_parent`；无法确认的关系进入人工核对清单。
5. 从现有成员关系生成不可变版本 Manifest、成员数、Schema Hash 和 Checksum。
6. 导出所有 ClickHouse App 表。
7. 导出所有 Milvus Lite Collection，记录源 DB、Collection、维度和距离度量。
8. 合并数据并检测：重复路径、重复向量、孤立向量、缺向量、已删除记录、维度不一致、模型版本不明、同名字段类型冲突和单位冲突。
9. 生成统一 Parquet Snapshot、字段注册表和版本 Manifest。
10. 先导入 PostgreSQL/OBS 事实层，再向候选检索引擎生成投影并构建索引。
11. 运行数量、Checksum、成员差异、字段分布、血缘和抽样内容校验。

### 10.3 增量追平

历史快照开始后，所有新变化写入 Outbox。目标引擎导入完成后按 `entity_version`重放增量，直到：

- Outbox Lag 为 0；
- 目标最大版本等于源最大版本；
- 所有 READY/PUBLISHED DatasetVersion 的 Manifest Watermark 与索引 Watermark 一致；
- Delete/ACL 事件无积压；
- 失败和死信均已处理。

### 10.4 短暂停机切换

维护窗口内执行：

1. 暂停上传、版本发布和元数据修改入口。
2. 停止产生新的 Embedding 任务。
3. 排空 `serial_ch`、Embedding 和 Indexer 队列。
4. 重放最后 Outbox，确认 Lag 为 0。
5. 对 App、DatasetVersion、Manifest、成员关系、Segment、删除、字段契约和权限执行最终对账。
6. 创建数据库备份/Snapshot。
7. 切换 Search API 配置和只读流量。
8. 执行冒烟查询与权限查询。
9. 恢复写入口并观察索引新鲜度。

### 10.5 回滚层级

| 层级 | 触发条件 | 回滚动作 |
|---|---|---|
| API 回滚 | 新 Search API 错误 | 路由回旧 API，数据写入继续进入 Outbox |
| 读流量回滚 | 查询质量/延迟异常 | 读回旧 ClickHouse + Milvus Lite，只保留目标引擎追数 |
| 写入回滚 | 目标消费者大量失败 | 暂停目标消费者，不回滚 PostgreSQL 业务事实 |
| 版本发布回滚 | 新版本成员、字段或索引异常 | 将 Published Pointer 原子切回旧 `version_id`，新版本标记为 DEPRECATED，保留不可变记录供排查 |
| 数据回滚 | 导入或 Schema 错误 | 清空候选索引并从 Snapshot 重建，不修改 OBS/PG |
| 模型回滚 | 新 Embedding 质量下降 | Alias/配置切回旧模型索引 |

旧检索系统至少保留一个完整验证周期的只读状态；确认回滚窗口结束后再归档 Milvus Lite 文件。

---

## 11. 安全、观测与运维改造

### 11.1 安全

- 禁止搜索 API 接收任意服务器目录路径；改为 App、DatasetVersion 和受控筛选条件。
- 所有搜索入口执行认证和 ACL 校验。
- `/health`不暴露模型文件路径、DB 文件路径和内部配置。
- 清缓存、清连接、重建索引等接口改为受保护的管理 API，不使用匿名 GET。
- 限制上传大小、Query 长度、Token 数、Top-K、超采样倍数和超时时间。
- 数据库账户按写入、查询、管理、备份分离。

### 11.2 监控指标

| 层面 | 指标 |
|---|---|
| 业务 | 查询量、无结果率、点击/采用率、NDCG 抽样 |
| API | P50/P95/P99、超时、错误、候选池大小、补召回次数 |
| Embedding | GPU 利用率、批量大小、推理延迟、失败率、模型版本 |
| 事件 | Outbox Lag、队列积压、重试、死信、版本差 |
| 版本控制 | 版本构建耗时、Checkpoint、Manifest成员差、发布等待、复用率、孤立对象 |
| 数据库 | CPU、内存、磁盘、Cache、Merge/Compaction、索引构建 |
| 质量 | ANN Recall 抽样、过滤正确率、跨库漂移、删除传播延迟 |
| 恢复 | 最近备份、恢复演练结果、预计 RPO/RTO、重建进度 |

### 11.3 队列与吞吐

不再使用全局 `serial_ch`限制所有检索写入：

- 按 `app_id`或 `hash(segment_id)`分区消费；
- 同一 Segment 依赖 `entity_version`保证新版本胜出；
- ClickHouse/Doris 批量写入；
- Qdrant 批量 Upsert；
- Embedding 与数据库写入独立扩容；
- 删除和 ACL 事件使用高优先级队列。

---

## 12. 推荐实施路线

### 12.1 第一阶段：共同基础，不绑定数据库

必须先完成：

1. `dataset_id/version_id/asset_id/asset_revision_id/segment_id/entity_version`稳定身份；
2. DatasetVersion不可变状态机、Manifest、父版本关系和成员模型；
3. `field_definition`字段注册、命名空间、类型、单位和合并策略；
4. `ingestion_run + chunk checkpoint`断点续写与幂等导入；
5. Embedding 模型元数据与统一 Parquet 快照；
6. PostgreSQL Outbox、索引 Watermark、死信、重放和对账；
7. Embedding Service 与 Search API 解耦；
8. `/v2/search`统一请求、结果和过滤 DSL；
9. 停止新增 Milvus Lite 文件。

这些改造即使最终数据库选择变化，也不会浪费。

### 12.2 第二阶段：并行 PoC，但设置先后顺序

1. **先做 Doris 4.0 PoC**：验证它能否同时满足 BM25 + ANN + SQL RRF 和单引擎管理。
2. **同时建立 ClickHouse 26.3 基准**：它是升级回退路线，也用于判断 Doris 迁移收益是否真实存在。
3. **仅当单引擎未通过向量门槛时，再做 Qdrant 完整 PoC**：同时部署集中式 Milvus 对照组；只有 Qdrant 产生可量化净收益才进入路线三，避免为更换产品而更换产品。

### 12.3 最终选择规则

- Doris 四个 Gate 全部通过：选择 Doris 4.0。
- Doris 未通过，但 ClickHouse 26.3 满足 Token 过滤 + Dense 排序业务验收：选择 ClickHouse 26.3。
- 单引擎 ANN 未通过，而业务不能降低 Recall/P99：比较集中式 Milvus 与 Qdrant；Qdrant 有显著净收益时选择 ClickHouse + Qdrant，否则保留集中式 Milvus 并终止 Qdrant 路线。
- 三者都未通过：不进行生产替换，保留 ClickHouse 元数据检索，先重构为集中式向量服务并重新定义 SLA。

### 12.4 当前最重要的非数据库工作

即使暂不确定最终引擎，也应立即启动以下改造：

- 用稳定 ID 替换路径身份；
- 建立不可变 DatasetVersion、Manifest、成员关系和版本血缘；
- 通过 Content Hash 与 AssetRevision 明确跨版本复用和修订边界；
- 建立字段注册表，停止继续扩展通用多类型 KV/EAV 表；
- 将多个 Milvus Lite DB 汇总成统一、可重建的数据快照；
- 建立多模态可检索文本生成链路；
- 建立索引事件和对账；
- 建立 Camera DMP 自有的混合检索标注集和基准测试。

这些工作的优先级高于直接安装任一新数据库，因为缺少它们时，三条路线都无法形成可靠的混合检索平台。

---

## 13. 官方资料与本地依据

### 13.1 本地依据

- `Camera DMP数据网格平台设计剖析与架构归档.md`
- `数据集版本管理设计.md`
- `数据平台对比分析报告.md`
- `platform_search_deployment/app0402.py`

### 13.2 ClickHouse

- [ClickHouse 26.3 LTS 发布说明](https://clickhouse.com/blog/clickhouse-release-26-03)
- [ClickHouse 全文检索 GA](https://clickhouse.com/blog/full-text-search-ga-release)
- [ClickHouse 2025 搜索能力演进与向量索引 GA](https://clickhouse.com/blog/clickhouse-2025-roundup)
- [ClickHouse 25.5 向量预过滤、后过滤与重评分](https://clickhouse.com/blog/clickhouse-release-25-05)
- [ClickHouse Vector Index 示例与演进](https://clickhouse.com/blog/alexey-favorite-features-2025)

### 13.3 Apache Doris

- [Apache Doris 4.0 发布说明](https://doris.apache.org/releases/v4.0/release-4.0.0/)
- [Doris 4.x Vector Index](https://doris.apache.org/docs/4.x/key-features/vector-index/)
- [Doris HNSW 与索引生命周期](https://doris.apache.org/docs/4.x/table-design/index/vector-index/hnsw/)
- [Doris Full-text Search](https://doris.apache.org/docs/dev/key-features/full-text-search/)
- [Doris SQL Reciprocal Rank Fusion](https://doris.apache.org/docs/4.x/key-features/reciprocal-rank-fusion/)
- [Doris Inverted Index](https://doris.apache.org/docs/4.x/key-features/inverted-index/)

### 13.4 Qdrant

- [Qdrant Hybrid and Multi-stage Queries](https://qdrant.tech/documentation/search/hybrid-queries/)
- [Qdrant Data Model and Payload](https://qdrant.tech/documentation/manage-data/)
- [Qdrant Distributed Deployment](https://qdrant.tech/documentation/scaling/distributed_deployment/)
- [Qdrant Scaling and Resilience](https://qdrant.tech/documentation/scaling/)
- [Qdrant Snapshot](https://qdrant.tech/documentation/operations/snapshots/)

---

## 14. 待实施前确认的数据

以下信息不影响当前路线设计，但在 PoC 配置和生产容量核算前必须取得：

1. Chinese-CLIP 实际输出维度、是否归一化、当前 Milvus 使用 IP 的准确原因；
2. 图片、视频、音频、文档中拥有 Caption/OCR/ASR/人工文本的比例；
3. 现网峰值查询并发、写入速率、每日新增 Segment 和删除率；
4. 最大 DatasetVersion 成员数量及常见过滤选择率；
5. 生产可提供的 CPU、RAM、NVMe、节点数和备份存储；
6. 当前 H3 查询函数、分辨率和查询模板清单；
7. 业务对关键词相关性排序的真实要求：Token 必须命中即可，还是必须独立 BM25 排名；
8. 现有 Dataset 和 DatasetVersion 的最大层级、父子/合并关系及版本创建频率；
9. 跨版本内容复用率、重复文件判定规则，以及是否已有可信的内容 Hash；
10. 当前动态字段数量、同名类型/单位冲突清单和需要长期保留的版本特定字段；
11. DatasetVersion 发布、回滚、废弃和物理清理的业务审批规则；
12. 全量版本重建与失败续写可接受的 RTO。

这些数据应作为 PoC 参数输入，而不是在缺少证据时由数据库厂商基准代替。
