# 数据平台重构中Lance的价值与引入建议

## 一、核心结论

Lance是面向多模态和AI数据的列式文件及表格式，适合管理经过清洗、标注、质检或特征提取后形成的版本化Dataset。它可以在同一数据体系中组织样本内容、标签、结构化属性和Embedding，并通过Manifest记录数据版本。Lance表格式本身支持Schema演进、版本快照和索引管理，但它不是业务数据库、对象存储或模型训练框架。[Lance Table Format](https://lance.org/format/table/)

结合现有数据平台，是否引入Lance主要取决于平台最终交付的内容：

- 如果平台主要提供数据搜索、筛选和文件打包交付，Lance不是必要组件。现有的ClickHouse、Milvus和OBS已经能够分别完成元数据查询、向量检索和原始文件交付。
- 如果平台需要持续生产训练集、评测集或经过多轮加工的派生数据集，Lance具有较高价值，可以增加一个版本化Dataset层。
- 如果两类业务同时存在，建议将Lance作为按需使用的Dataset存储，不应将其作为全平台唯一的数据格式，也不应直接替换PostgreSQL、OBS和ClickHouse。

因此，对新平台的建议不是全面采用Lance，而是根据训练和评测数据集的实际需求决定是否建设Lance Dataset层。

## 二、Lance在现有平台中可以做什么

现有平台采用多组件分工的方式管理数据：

- OBS/MinIO保存图片、视频、音频和压缩包等原始文件；
- ClickHouse保存文件路径、数据类型、质量属性和数据集关联等可搜索元数据；
- Milvus保存Embedding并提供向量检索；
- PostgreSQL保存用户、权限、任务、数据集和版本等业务信息；
- Parquet和JSON用于批量入库、数据交换、备份或交付清单。

这种架构适合持续接入、搜索和交付数据。但是，当一个完整样本同时包含多张图片、文本、标签、质量分和Embedding时，数据会分布在多个系统中。例如，一个图像编辑评测样本可能包含：

```text
sample_id
source_image
reference_image
target_image
prompt
quality_score
human_label
embedding
model_version
generation_config
```

在现有架构下，图片位于OBS，检索属性位于ClickHouse，Embedding位于Milvus，版本和任务信息位于PostgreSQL。平台需要通过稳定ID将这些信息组合为一个完整样本，并保证各系统中的版本一致。

引入Lance后，可以在原始数据层之上增加一个Dataset层：

```text
OBS/MinIO原始数据
        ↓
清洗、过滤、标注、质检、Embedding
        ↓
Lance Dataset
        ├── 样本ID及多模态关系
        ├── 文件内容或对象存储引用
        ├── 文本、标签和质量属性
        ├── Embedding
        ├── 模型与任务信息
        └── Dataset版本
```

在现有平台中，Lance主要可以承担三项工作。

### 1. 统一组织完整样本

Lance可以按照稳定样本ID组织图片、文本、标签、质量分和Embedding。一个样本需要包含哪些字段，由明确的Schema定义，不再完全依赖文件路径和多个系统之间的临时关联。

这并不意味着原始文件必须全部从OBS迁移到Lance。对于超大视频、压缩包和长期归档文件，可以继续保存在OBS中，由Lance记录稳定引用和内容哈希；对于需要频繁批量读取的图片、文本和标注，可以根据性能测试决定是否在Lance Dataset中物化。

### 2. 保存加工后的数据集版本

平台可以将数据加工过程中的不同结果保存为独立Dataset版本，例如：

```text
原始版 → 去重版 → 质量过滤版 → 自动标注版 → 人工审核版 → 评测版
```

每个版本可以记录其精确上游版本、Schema、处理任务、模型版本和参数。这样能够明确回答某个训练集或评测集从何而来，以及能否重新得到相同结果。

### 3. 向训练和评测程序提供完整数据

训练或评测程序可以按批次从固定Lance版本中读取图片、文本、标签和质量信息，减少分别查询OBS、ClickHouse和Milvus后再组合样本的过程。

模型本身不能直接使用Lance文件完成训练。实际流程仍然需要DataLoader读取批次、解码图片或音频、执行预处理，并转换为模型需要的Tensor。Lance的作用是管理Dataset和提高数据读取及关联效率，而不是替代DataLoader或训练框架。

同时，Lance不负责以下工作：

- 用户、权限、审批和业务事务；
- 原始文件的长期归档；
- 实时运营统计和复杂聚合；
- 消息队列、任务调度和模型执行；
- 自动完成OCR、ASR、Caption、标注或Embedding。

## 三、什么情况下值得引入Lance

| 业务情况 | Lance价值 | 判断依据 |
|---|---:|---|
| 用户搜索文件后，由平台从OBS打包交付 | 低 | ClickHouse/Milvus负责检索，OBS负责交付，Lance不会明显缩短主要链路 |
| 需要固定一次查询或交付的文件清单 | 低 | 使用Parquet或JSON保存文件ID、路径、内容哈希和元数据版本即可复现交付结果 |
| 数据主要用于原始归档、备份或长期保存 | 低 | OBS/MinIO更适合保存原始对象，Lance会增加格式转换和维护成本 |
| 主要需求是设备统计、质量分布、趋势分析和运营大屏 | 低 | ClickHouse更适合高并发筛选、统计和复杂聚合 |
| 现有ClickHouse和Milvus已经稳定满足搜索需求 | 较低 | 仅为减少组件数量引入Lance，收益可能不足以抵消迁移和维护成本 |
| 数据需要多轮清洗、过滤、标注、质检和增强 | 高 | 需要保存派生数据、处理关系和多个可追溯版本 |
| 平台需要构建固定的训练集、评测集或Benchmark | 高 | 需要确保样本内容、标签、模型信息和输入版本可复现 |
| 一个样本包含多张图片、文本、标签、质量分和Embedding | 高 | Lance可以按照稳定ID统一组织完整多模态样本 |
| 训练或评测程序需要频繁批量读取完整样本 | 高 | 可以减少跨OBS、ClickHouse和Milvus组合数据的复杂度 |
| 需要同时支持样本级过滤、全文检索和向量检索 | 中到高 | Lance可以让数据和索引绑定到同一Dataset版本，但仍需验证并发和延迟要求 |

是否引入Lance，可以归纳为两个核心判断：

1. 平台的主要交付物是原始文件，还是具有明确Schema和版本的Dataset？
2. 数据是否需要经过多轮加工，并被训练或评测程序反复读取？

如果两个问题的答案分别是“原始文件”和“不需要”，则没有必要将Lance作为新平台核心组件。如果答案是“版本化Dataset”和“需要”，则Lance具有明确的引入价值。

## 四、重构建议

建议在重构中保持各组件职责清晰：

```text
PostgreSQL
用户、权限、任务、资源和版本登记

OBS/MinIO
原始文件、压缩包和长期存储

ClickHouse
在线元数据搜索、统计和聚合

Milvus
高并发或大规模向量检索

Parquet/JSON
数据交换、导出、备份和交付清单

Lance（按需引入）
训练集、评测集和多轮加工后的版本化Dataset
```

如果新平台仍以搜索、筛选、申请和文件交付为主要业务，建议优先完善以下能力，而不是引入Lance：

- 为每个文件建立稳定ID和内容哈希；
- 为每次交付保存不可变文件清单；
- 建立PostgreSQL、ClickHouse、OBS和Milvus之间的统一提交和校验机制；
- 完善版本、血缘、失败补偿和跨系统恢复能力。

如果新平台将训练集、评测集和多模态数据加工结果作为核心数据产品，则建议在OBS Raw层之上增加Lance Dataset层，并通过小规模PoC验证：

- OBS/MinIO兼容性与提交一致性；
- 批量写入和训练读取吞吐；
- 图片、视频及大Blob的组织方式；
- 并发写入、Fragment整理和索引维护；
- 版本保留、存储放大和旧版本清理；
- 是否仍需保留Milvus作为独立在线向量服务。

Lance使用不可变Fragment和版本Manifest管理数据。多次小规模追加会产生较多Fragment，删除和更新也会保留旧版本引用，因此生产环境需要规划Compaction和版本清理策略。[Lance读写与维护](https://lance.org/guide/read_and_write/)

最终建议如下：

> 如果新平台的核心业务仍然是搜索和文件交付，Lance不是必要组件；如果平台需要持续生产、管理和复现训练集、评测集以及多模态派生数据，建议引入Lance，但仅将其定位为Dataset层，而不是替代现有全部存储与检索组件。

## 参考资料

- [Lance Table Format](https://lance.org/format/table/)
- [Lance数据读写与维护](https://lance.org/guide/read_and_write/)
- [Lance对象存储配置](https://lance.org/guide/object_store/)
- [LanceDB搜索能力](https://docs.lancedb.com/search/)

