# 抽取阶段两轮化重构执行计划

> 整理日期：2026-08-17（2026-08-18 依据外部项目调研修订）
> 当前分支：`smarter-extract`
> 前置阅读：`EXTRACTION_NAME_MERGE_NOTES.md`（问题背景与实验记录）、`EXTRACTION_REFERENCE_PROJECTS.md`（外部项目调研）

## 0. 目标与原则

### 目标

把现有"第一阶段抽取 → 第二阶段合并裁决 →（原型中的）第三轮译名统一"改造为两轮：

```
第一阶段：分块抽取（不变）
    ↓
本地预处理（无 AI）：归一化 → 频率统计 → 敬称剥离 → 连通分量成簇 → 零独立出现折叠
    ↓
第二阶段：按簇发送（带频率证据），AI 一次完成 类型裁决 + 译名统一
    ↓
最终阶段：本地兜底 + 敬称变体译名回填
```

### 原则（不可违反）

1. **永远不删除任何在原文中独立出现的 surface form**。所有第一阶段抽到的 source 都必须出现在最终词表中，两个例外：a) AI 判定为无价值普通词汇的孤立词（见 3.4）；b) `independent_count == 0` 的短词——它在原文中从不脱离长词独立出现，本地折叠进长词（见 1.2b），这是确定性规则而非 AI 裁决。
2. **实体合并与译名统一是两个问题**。AI 不回答"是否同一实体并删除谁"，只回答"类型是什么、共享片段译法怎么统一"。
3. **本地能确定性推导的不发 AI**（敬称变体、分隔符对齐）。
4. **簇是分批的原子单位**，不允许跨批拆散。
5. **校验失败必须有降级路径**，退化为"不统一"而不是丢词。

---

## 1. 阶段一：本地预处理模块（无 AI，纯函数，可单测）

新建文件 `ModuleFolders/Service/TaskExecutor/ExtractionClustering.py`，全部实现为模块级纯函数或无状态类，不依赖 `AnalysisTask` 实例，方便直接用 pytest 测试。

### 1.1 归一化 `normalize_source(source: str) -> str`

只用于**分组比较**，不修改词条实际存储的 source：

```python
_NORMALIZE_TABLE = str.maketrans({
    "･": "・",   # 半角中点 → 全角中点
    "·": "・",   # 间隔号 → 全角中点（仅比较用）
    "＝": "=",
    "　": " ",   # 全角空格 → 半角
})
def normalize_source(source: str) -> str:
    return source.translate(_NORMALIZE_TABLE).strip()
```

实现细节：

- 维护 `normalized → [原始 source 列表]` 的反查映射。归一化后相同的多个原始 source（如 `ヴェーネ・アンスバッハ` 和 `ヴェーネ･アンスバッハ`）进入同一簇，且互相视为"必须同译名"的硬约束。
- **不做** NFKC 全量归一化——会把 `％s`、`＜b＞` 这类禁翻 marker 弄坏；只白名单式转换分隔符和空格。
- 候选比较、包含判断全部基于 normalized 形式；发给 AI 和写回缓存时保留原始形式。

### 1.2 真实频率统计与零独立出现折叠

**背景**：现有 `occurrence_count` 是"多少个分块抽到该候选"（notes 2.4），不可用于任何决策。全部原文就在手里，直接本地统计真实频率。

新增两个指标，全部确定性计算：

```python
def count_frequencies(sources: list[str], full_text: str) -> dict[str, Frequency]:
    """
    total_count:       source 在原文中的出现总次数（普通子串计数）
    independent_count: source 出现位置中，不被同簇任何更长 source 覆盖的次数
    """
```

实现细节：

- `full_text` 由所有分块原文拼接（`generate_analysis_source_chunks` 的输入侧已有全量文本，注意用 normalized 形式统计，与分组比较一致）。
- `total_count` 用 `str.count` 或 `re.finditer` 逐词统计即可；词数几千、文本几 MB 的量级下纯 Python 可接受，实测过慢再换 Aho-Corasick 单趟扫描。
- `independent_count` 计算：对簇内每个 source 收集所有出现区间 `[start, end)`，短词的某次出现若被任何长词的出现区间完全覆盖，则不计入 independent。只需在簇内比较（跨簇不存在包含关系）。

**零独立出现折叠（关键规则）**：

```
independent_count(short) == 0 且 short 与某长词在同一簇
→ short 在原文中从不独立出现，是第一阶段抽取的副产物
→ 本地折叠：词条不独立输出，candidates 并入折叠目标
→ 折叠关系记入 collapse_targets: {short: [long1, long2, ...]}，日志记录
```

**多父折叠**（调研 16.2 指出的缺陷修正）：short 的出现可能被多个不同长词分别覆盖（`Arthur` 的 30 次出现，20 次在 `Arthur King` 里、10 次在 `Sir Arthur` 里）。此时：

- `collapse_targets` 记录**全部**覆盖它的长词及各自覆盖次数，不做单父假设；
- candidates 并入覆盖次数最多的长词（只影响证据归属，成本低、无语义损失——同簇长词都会在同一发送单元里见到这些证据）；
- 折叠记录写进最终 `analysis_data` 的诊断字段（新增 `collapsed: [{source, targets, reason}]`），用户可查证哪些词被折叠及原因。

这是旧版贪心合并**唯一正确的适用场景**，现在变成有证据的确定性规则：

- `Arthur` 30 次全部在 `Arthur King` 内 → 折叠，翻译阶段也不会有双命中问题。
- `アンスバッハ` 50 次中有 10 次独立出现 → 必须保留为独立词条。

边界处理：

- 折叠不跨敬称边（敬称变体已有独立的回填机制）。
- `total_count == 0` 的 source（第一阶段幻觉产物或原文归一化差异导致找不到）：保留词条但标注 `note: 原文未命中`，计数记 1，不参与折叠判断。
- 两个指标随词条写入最终结果，`occurrence_count` 字段的取值改为 `total_count`（语义修正顺带完成，notes 9.6 关闭）。

### 1.3 敬称剥离 `strip_honorific(source, known_sources) -> str | None`

敬称表按源语言组织，从 `self.config.source_language` 选择（`TaskConfig` 已有该字段，与 `_get_recommended_translation_language_requirement` 取 `target_language` 的方式一致）：

```python
HONORIFIC_SUFFIXES = {
    "japanese": [
        # 长的在前，避免 さま 被 ま 之类误匹配；只做单层剥离
        "さん", "さま", "様", "くん", "君", "ちゃん", "たん",
        "殿", "どの", "氏", "先生", "先輩", "嬢", "卿",
        "陛下", "殿下", "閣下", "お嬢様", "夫人",
    ],
    "korean": ["씨", "님", "군", "양"],
    # 其他语言先留空，命中不到就自然跳过
}
```

规则（**base-must-exist 硬约束**）：

```python
def strip_honorific(source: str, known_sources: set[str], suffixes: list[str]) -> str | None:
    for suffix in suffixes:               # 已按长度降序排列
        if source.endswith(suffix):
            base = source[: -len(suffix)]
            if len(base) >= 2 and base in known_sources:
                return base               # 返回 base，建立 variant→base 边
    return None
```

实现细节：

- `len(base) >= 2`：防止 `お母さん → お母`、单字名误剥离。日文单字名（`凛` 等）+ 敬称的 case 会漏掉，可接受——漏掉的代价只是该变体独立成词条，不丢数据。
- **只剥一层**，不递归（`アーサー様さん` 不存在于正常文本）。
- `known_sources` 用 normalized 形式。
- 剥离结果记录为 `honorific_edges: dict[variant_source, (base_source, honorific)]`，同时保存检测到的敬称字符串，后续两个用途：
  1. 变体不进入 AI 输出要求（见 3.3），译名由 base 推导；
  2. 敬称作为性别/身份证据传给 AI（`嬢/お嬢様 → 女性`，`卿/陛下 → 高地位`），放进 candidate 的 note 附加字段。

### 1.4 连通分量成簇 `build_clusters(sources) -> list[list[str]]`

替换现有贪心挂靠（`AnalysisTask.py:400-425` 的排序+consumed_sources 逻辑）：

```
节点：所有 normalized source
边：
  a. 包含边：normalized(short) in normalized(long) 且 len(short) >= 2
  b. 敬称边：honorific_edges 中的 variant → base
用 union-find 求连通分量
```

实现细节：

- **包含边加最小长度门槛**：`len(short) >= 2`（对 CJK）。单字 source（如 `凯`）不产生包含边，避免 `凯/凯尔` 这类 notes 2.3 的误连——单字词自成一簇或只通过敬称边入簇。这是有意的保守取舍：单字名与全名的译名统一交给 AI 看到 candidates 后在簇外自然处理不了，接受不统一，优先防误合。
- 包含判断复杂度 O(n²) 字符串包含。第一阶段去重后 source 量级在几百到几千，n² 次 `in` 操作在纯 Python 下毫秒~百毫秒级，不需要 Aho-Corasick；若将来超过 1 万条再优化。
- **语义簇与发送单元分离**（调研 16.3 指出的概念矛盾修正）：
  - **语义簇** = 连通分量，无大小上限，是频率统计、折叠判断的作用域；
  - **发送单元（reduction unit）** = 一次发给 AI 的词条组，上限 `MAX_UNIT_SIZE = 30`。
  - 语义簇 ≤ 30 时，发送单元 = 语义簇。超过时（共享姓氏的巨簇）拆分为多个发送单元，拆分规则：共享片段（如姓氏 `Smith`）词条进入**每个**发送单元——在自己所属的单元里作正常 entry，在其他单元里作只读 `context_entries`（标注"译名已在其他批次裁决/待裁决，本批仅供参考对齐"）。这样"簇不可拆"修正为"**共享片段的译名证据不可拆**"，语义正确且可实现。
  - 同一语义簇的多个发送单元**顺序执行**（不并发）：后发的单元把先前单元已裁决的共享片段译名放进 context_entries，实现跨单元一致。
- 输出结构：

```python
@dataclass
class Cluster:
    members: list[str]            # 原始 source，按长度降序（已剔除被折叠词）
    honorific_variants: dict[str, tuple[str, str]]  # variant → (base, honorific)
    normalized_dups: dict[str, list[str]]           # normalized → 原始形式们
    collapse_targets: dict[str, list[tuple[str, int]]]  # 折叠：short → [(long, 覆盖次数)]
    frequencies: dict[str, tuple[int, int]]         # source → (total, independent)
    contain_edges: list[tuple[str, str, str]]       # (short, long, shared_fragment)，供 relations 传递
```

执行顺序注意：折叠判断（1.2）需要簇内包含关系，因此实际流程是 成簇 → 簇内频率统计 → 折叠 → 剔除被折叠成员 → 发送单元划分，`build_clusters` 内部编排。

### 1.5 单元测试

新建 `Tests/test_extraction_clustering.py`（项目当前没有测试目录，创建之，用 pytest，不依赖 Qt/LLM）：

- 归一化：`・/･` 同簇；`％s` 不被改变。
- 敬称：`アーサーさん` + `アーサー` 建边；`お母さん` 单独存在时不剥离；`王様` 不剥离（`王` 单字被长度门槛挡住）。
- 连通分量：notes 2.2 的 `Arthur King / Sir Arthur / Arthur` 三者成一簇（对比旧贪心只能二选一）。
- 频率与折叠：构造文本验证 a) `Arthur` 全部被 `Arthur King` 覆盖 → independent=0 → 折叠；b) 部分独立出现 → 保留；c) `total_count == 0` 幻觉词 → 保留且不折叠；d) 三层嵌套（`ヴェーネ・アンスバッハ・セカンド` ⊃ `ヴェーネ・アンスバッハ` ⊃ `ヴェーネ`）时覆盖判断以最长实际出现为准；e) 多父折叠：`Arthur` 同时被 `Arthur King` 和 `Sir Arthur` 覆盖 → collapse_targets 记录两者及覆盖次数。
- 巨簇拆分：构造 35 个共享姓氏的名字，验证语义簇保持完整、发送单元 ≤ 30、共享姓氏词条出现在每个发送单元（一处为 entry、其余为 context_entries）。
- 用 `sb_after.json`（已在仓库根目录，加入 `.gitignore` 或拷贝精简样本进 `Tests/fixtures/`）做一个快照测试：验证 344 条记录成簇数量与成员稳定。

---

## 2. 阶段二：改造 `_prepare_reduction_batches`

位置：`AnalysisTask.py:364`。

### 2.1 新的输入结构

`raw_grouped_inputs` 收集逻辑不变（`AnalysisTask.py:366-383`）。之后：

```python
clusters = ExtractionClustering.build_clusters(
    raw_grouped_inputs.keys(),
    source_language=self.config.source_language,
)
```

第二阶段的传输单元从单个 Group 变为发送单元（reduction unit，见 1.4）：

```python
{
    "cluster_id": 3,
    "entries": [
        {"entry_id": 101, "source": "ヴェーネ・アンスバッハ", "count": 5,   "independent_count": 5,   "candidates": [...]},
        {"entry_id": 102, "source": "アンスバッハ",           "count": 50,  "independent_count": 10,  "candidates": [...]},
        {"entry_id": 103, "source": "ヴェーネ",               "count": 200, "independent_count": 195, "candidates": [...]},
    ],
    "relations": [
        {"type": "contains", "short": 102, "long": 101, "shared_fragment": "アンスバッハ"},
        {"type": "contains", "short": 103, "long": 101, "shared_fragment": "ヴェーネ"},
    ],
    "context_entries": [],   # 巨簇拆分时，其他发送单元的共享片段词条（只读参考）
    # 被折叠词（independent_count==0）不作为 entries 出现，candidates 并入折叠目标
    # 敬称变体不作为 entries 出现，其 candidates 并入 base 的 candidates，
    # 但在 base entry 上标注：
    # {"source": "アーサー", "honorific_variants": ["アーサーさん", "アーサー様"], ...}
}
```

采纳调研 §7（LocFlow）/§18 的三点：

- **entry_id**：全局递增整数。CJK source 经模型 echo 可能出现全半角/不可见字符漂移导致校验误杀，数字 ID + source 双重对齐更稳。
- **relations 显式传递**：包含边和 shared_fragment 本地已算出（Cluster.contain_edges），直接告诉 AI，比让它自己重新发现关系可靠。
- **候选投票聚合**（调研 §4.4 GalTransl）：candidates 不再原样罗列，按 `(type, recommended_translation)` 聚合为 `{type, recommended_translation, vote_count, chunk_count, gender, category_path, note}`，note 取最长非空。同一译名被 8 个分块提出 → 一条 `vote_count: 8`，压缩 token 且把候选稳定度变成显式证据。

count（=total_count）和 independent_count 来自 1.2 的本地统计，与 vote_count 一起作为 AI 选择统一译法的证据（高频独立形式、高票译名应优先胜出）。

### 2.2 状态字段变更

- `self.grouped_stage_two_inputs`：保留（`_finalize_results` 和 `_get_candidate_occurrence_count` 依赖它），键改为**每一个独立 source**（含敬称变体与被折叠词），值结构不变。
- `self.grouped_stage_two_source_aliases`：语义从"短词→长词主源"改为"敬称变体→base"与"被折叠词→折叠目标"的并集。非变体的 source 全部自映射。`_finalize_results:548` 的 alias 解析逻辑可以原样复用。
- 新增 `self.honorific_variant_map: dict[variant, (base, honorific)]` 供最终阶段回填。
- 新增 `self.source_frequencies: dict[source, (total, independent)]` 供最终阶段写入 `occurrence_count`。

### 2.3 分批（发送单元为原子）

替换 `AnalysisTask.py:430-439`：

```python
for unit_payload in reduction_units:            # 发送单元按 token 计量
    tokens = Tokener().num_tokens_from_str(json.dumps(unit_payload, ...))
    if tokens > self.REDUCE_BATCH_TOKEN_LIMIT:
        # 极端情况：单个发送单元超限 → 走 1.4 的巨簇拆分逻辑再拆一层，记 warning
        ...
    if current_batch and current_tokens + tokens > limit:
        batches.append(current_batch); reset
    current_batch.append(unit_payload)
```

- 发送单元是分批原子；1.4 的 `MAX_UNIT_SIZE` + 投票聚合已让单元超限几乎不可能。
- **并发约束**：来自同一语义簇的多个发送单元需顺序执行（1.4 的跨单元一致机制）；不同语义簇的单元照常并发。实现：拆分过的巨簇单元串成链提交（前序 future 完成后把裁决出的共享片段译名写入后续单元的 context_entries 再提交），普通批次维持现有线程池并发。

### 2.4 开关处理

- 删除 `extract_short_name_merge_switch` 的分支逻辑（`AnalysisTask.py:385-425` 的 if/else 合并为新流程）。
- 配置键保留但改名策略见第 5 节。

---

## 3. 阶段三：改造二阶段 prompt / 协议 / 校验

### 3.1 新协议

输入为 2.1 的发送单元列表（含 entry_id、count/independent_count、聚合后的 candidates、relations、context_entries）。

输出：

```json
{
  "characters": [
    {"entry_id": 101, "source": "ヴェーネ・アンスバッハ", "recommended_translation": "维内·安斯巴赫", "gender": "女性", "note": "..."},
    {"entry_id": 103, "source": "ヴェーネ", "recommended_translation": "维内", "gender": "女性", "note": "..."}
  ],
  "terms": [
    {"entry_id": 102, "source": "アンスバッハ", "recommended_translation": "安斯巴赫", "category_path": "身份", "note": "贵族姓氏"}
  ],
  "discarded": [{"entry_id": 205, "source": "普通词汇"}]
}
```

### 3.2 Prompt 关键规则（`_build_second_stage_prompt` 重写）

```
1. 完整返回：每个 entry 的 entry_id 和 source 必须在 characters/terms/discarded
   三处恰好出现一次。entry_id 和 source 必须原样返回，不得新增、修改或遗漏。
2. 唯一归属：同一 entry 只能是角色或术语之一。
3. 译名一致：relations 中标注了名称之间的包含关系和共享片段（姓氏、名、称号），
   共享片段必须使用相同译法。姓氏与全名不是同一人，仍需分别保留词条。
4. 频率与投票证据：count 是该词在原文中的真实出现次数，independent_count 是
   不被更长名称覆盖的独立出现次数，vote_count 是该译名被多少个文本分块独立提出。
   统一译法冲突时，优先采用 independent_count 和 vote_count 更高的既有译名。
5. 只读参考：context_entries 中的词条已在其他批次处理，仅用于对齐共享片段译法，
   不要为它们输出结果。
6. 谨慎丢弃：只有确定是普通词汇（如"今天""然后"）才放入 discarded。
   人名、地名、称号、姓氏一律不丢弃。
7. 信息整合：多个 candidates 冲突时选最合理的译名/分类/备注。
```

Few-Shot 更新（替换 `AnalysisTask.py:465-508` 的 sample）：用 `ヴェーネ` 簇做正例，展示"姓氏保留为 term + 两个全名保留为 character + 共享片段译名统一 + entry_id 原样返回"；再加一个含 discarded 的普通词单元。这是防止模型退回"只输出主 source"旧行为的关键，few-shot 必须展示**输入 3 个 entry、输出 3 条结果**的形态，且输入里带 relations 和 vote_count，让模型看到证据字段的用法。

### 3.3 敬称变体的处理

- 变体（`アーサーさん`）**不进 entries**，只在 base entry 上列 `honorific_variants` 字段供 AI 参考（性别证据）。
- prompt 中说明：`honorific_variants 仅供判断参考，不需要为其输出结果`。
- 好处：输出 token 减少，变体译名的名字部分 100% 与 base 一致，敬称后缀沿用第一阶段译法（本地推导，见 4.2）。

### 3.4 Validator（`_run_second_stage` 中替换 `stage_two_validator`）

对每个 batch 严格校验：

```python
def stage_two_validator(parsed):
    # entry_id 与 source 双重对齐（调研 §7/§15.2）
    expected = {entry["entry_id"]: entry["source"]
                for unit in batch for entry in unit["entries"]}
    returned = collect(parsed["characters"]) + collect(parsed["terms"]) \
             + collect(parsed.get("discarded", []))
    returned_ids = [item["entry_id"] for item in returned]
    if set(returned_ids) != set(expected): return False, "entry_id 集合与输入不一致"
    if len(returned_ids) != len(set(returned_ids)): return False, "entry_id 出现多次"
    for item in returned:
        if item["source"] != expected[item["entry_id"]]:
            return False, f"entry_id {item['entry_id']} 的 source 与输入不符"
    # context_entries 是只读的，不得出现在输出
    # 多成员发送单元不允许 discard（防止 AI 借 discarded 恢复旧合并行为）
    for unit in batch:
        if len(unit["entries"]) > 1:
            if any(e["entry_id"] in discarded_ids for e in unit["entries"]):
                return False, "多成员单元不允许丢弃成员"
    return True, ""
```

多成员单元禁止 discard 是关键防线：AI 想"合并"只能通过 discard，堵住这条路就堵住了旧行为。孤立单词单元仍允许 discard（保留清理普通词汇的能力）。entry_id 对齐后，source 轻微 echo 漂移（全半角等）可定位到具体词条并按输入 source 修复，而不是整批重试——validator 对"仅 source 漂移、entry_id 正确"的情况做自动修正 + warning，不算失败。

### 3.5 降级路径（失败阶梯）

采纳调研 §15.3"失败时拆小，而不是整批放弃"，在现有 重试→兜底 之间加一层拆批：

```
整批请求失败（2 次重试用尽）
    ↓
batch 含多个发送单元？ → 对半拆成两个子批，各自重试一次（递归一层，不无限递归）
    ↓
单个发送单元仍失败
    ↓
本地兜底：_finalize_results 第 2 步启发式逐词出结果（不统一译名），warning 日志
```

最后一层兜底不需要新代码：现有 `_finalize_results` 第 2 步（`AnalysisTask.py:568-601`）已覆盖"AI 没给结果的 source"——因为 2.2 中 `grouped_stage_two_inputs` 的键是每个独立 source，兜底自动变成"逐词独立出结果"。拆批层实现在 `_run_second_stage` 外包一个 `_run_second_stage_with_split(batch, depth=0)`，约 20 行。

---

## 4. 阶段四：最终阶段改造

### 4.1 `_finalize_results` 调整

- 第 1 步吸收 AI 结果：alias 解析行为不变（现在 alias 是敬称变体映射 + 折叠映射，AI 不会返回这两类 source，映射实际只在兜底路径生效）。
- 新增：记录 AI 返回的 `discarded` 集合，兜底循环跳过这些 source（当前代码会把 AI 有意丢弃的词又捡回来——旧版是 bug 级行为，本次一并修正；只对孤立簇生效，与 3.4 的校验闭环）。
- `occurrence_count` 改为写入 `self.source_frequencies` 中的 `total_count`（替换 `_get_candidate_occurrence_count` 的分块计数语义；该方法保留用于频率统计缺失时的兜底）。被折叠词的 total_count 计入折叠目标词条的 note 不必要，直接丢弃即可——折叠目标自身的 count 已包含覆盖出现。

### 4.2 敬称变体回填（新增 `_backfill_honorific_variants`）

在 `_finalize_results` 返回前执行：

```python
for variant, (base, honorific) in self.honorific_variant_map.items():
    base_row = merged_characters.get(base) or merged_terms.get(base)
    if not base_row: continue                      # base 被 discard → 变体走兜底
    target_table = 同 base 所在表
    target_table[variant] = {
        "source": variant,
        "recommended_translation": base_row["recommended_translation"] + honorific_translation(honorific),
        ... 复制 base 的 gender/category/note，note 追加 "敬称变体，基于 {base}",
        "occurrence_count": self._get_candidate_occurrence_count(variant, type),
    }
```

`honorific_translation` 敬称译法映射，第一版硬编码在敬称表旁边：

```python
HONORIFIC_TRANSLATION_ZH = {
    "さん": "先生/小姐占位 → 第一版直接沿用变体自己第一阶段的译名后缀",
}
```

**实现取舍**：敬称→目标语译法其实依赖性别和风格（さん→先生/小姐/桑）。第一版不做映射表，改用更稳的策略——取该 variant 第一阶段候选译名中的后缀部分：`variant_stage1_translation` 若以 `base_row 译名`（或旧译名）开头则直接替换前缀；否则整体用 `base译名 + (variant原译名去掉base旧译名前缀)`；再兜底直接用 base 译名拼变体第一阶段译名的差异部分失败时，退化为保留 variant 第一阶段原译名并只在 note 标注不一致。逻辑封装成独立纯函数 `derive_variant_translation(base_old, base_new, variant_old) -> str`，可单测。

### 4.3 删除第三轮原型

移除 `AnalysisTask.py:627-754` 四个方法及 `run()` 中 166-167 行的调用：

- `_prepare_translation_unification_batches`
- `_build_translation_unification_prompt`
- `_run_translation_unification_stage`
- `_unify_contained_source_translations`

---

## 5. 阶段五：配置与 UI

### 5.1 配置键

- 新键 `extract_name_cluster_mode: str`，取值：
  - `"cluster"`（新默认）：本计划的完整流程。
  - `"legacy_merge"`：旧版贪心合并（保留 `AnalysisTask.py:400-425` 逻辑于独立方法 `_legacy_greedy_merge` 中，过渡 1-2 个版本后删）。
- 旧键 `extract_short_name_merge_switch` 迁移：读取配置时若存在旧键且为 `True` 映射到 `legacy_merge`，为 `False` 映射到 `cluster`，然后写回新键。迁移逻辑放在 `ExtractionSettingsPage` 的 default 处理处。

### 5.2 UI（`ExtractionSettingsPage.py`）

- 把 `add_widget_short_name_merge` 的 `SwitchButtonCard` 换成 `ComboBoxCard`（组件已存在：`UserInterface/Widget/ComboBoxCard.py`）。
- 两个选项文案：
  - `智能成簇（推荐）`：保留所有长短名称与敬称变体，AI 在裁决时统一译名。
  - `旧版自动合并`：短名称合并进长名称，只保留最长词条。
- `Resource/Localization/AdvancedSettings.json`：删除本分支新增的两条旧文案，新增选项文案的四语翻译（简中/繁中/English/日本語，格式照抄现有条目）。

---

## 6. 阶段六：验证

### 6.1 单元测试（无网络）

- `Tests/test_extraction_clustering.py`：见 1.5。
- `Tests/test_variant_translation.py`：`derive_variant_translation` 的各分支。
- `Tests/test_stage_two_validator.py`：把 validator 提成可独立调用的函数后测：entry_id 缺失/重复/多成员单元 discard 均被拒；"entry_id 正确但 source 轻微漂移"被自动修正而非拒绝；context_entries 出现在输出被拒。
- `Tests/test_candidate_aggregation.py`：投票聚合——8 条相同 `(type, 译名)` 候选 → 1 条 `vote_count: 8`；不同译名分列并保留各自票数。

### 6.2 集成验证（手动，一次性）

- 用 `sb_after.json` 对应的原项目重跑分析，核对：
  1. `ヴェーネ` 簇中 independent_count > 0 的 source 全部保留且姓氏片段译名一致；
  2. 敬称变体（若有）译名的名字部分与 base 最终译名一致，敬称后缀保留其第一阶段译法（如 アーサーさん → 亚瑟先生，名字部分随 base 统一，"先生"来自第一阶段翻译）；
  3. 词条总数 ≈ 旧版关闭合并时的 344 条，减少的部分只能来自 discarded 与零独立出现折叠（逐条抽查折叠日志确认合理）；
  4. `occurrence_count` 抽查若干词条与原文实际次数一致；
  5. 二阶段请求数与旧版持平（无第三轮）。
- 找一个含大量 `～さん/～様` 的日文样本项目验证敬称路径。

### 6.3 回归风险点自查清单

- [ ] `_get_candidate_occurrence_count` 在新键结构下仍返回合理值（键粒度变细，行为应更准）。
- [ ] 停止任务（STOPING）在成簇/回填阶段可中断。
- [ ] 二阶段整批失败 → 兜底逐词出结果，不抛异常。
- [ ] 非日文项目（无敬称命中）行为等价于"归一化 + 连通分量"。
- [ ] `non_translate` 流程完全不受影响。

---

## 7. 实施顺序与提交切分

按依赖顺序，每步一个 commit，均可独立回滚：

1. `feat: add ExtractionClustering module with tests`（阶段 1，纯新增，零风险）
2. `feat: stage-one debug dump and replay tooling`（第 8 节，先有调试工具再做改造）
3. `refactor: stage-two batching uses clusters`（阶段 2，含 legacy 路径保留）
4. `feat: cluster-aware stage-two protocol and validator`（阶段 3）
5. `feat: backfill honorific variants and drop unification prototype`（阶段 4）
6. `feat: replace merge switch with cluster mode option`（阶段 5 + 文案）
7. 手动集成验证（阶段 6.2），问题修复后合入 main

前置清理：当前工作区未提交的第三轮原型改动（`AnalysisTask.py` 中 `_unify_*` 等）在步骤 4 会被删除，建议先 `git stash` 或提交到本分支留档，避免重构时混淆。

---

## 8. 开发辅助：第一阶段结果快照与重放

**动机**：第一阶段全文抽取是几十上百次并发请求，昂贵且慢；二阶段裁决请求少，是本次重构的主要调试对象。需要"跑一次第一阶段、无限次重放调试后续阶段"的能力。

### 8.1 快照保存（debug dump）

在 `run()` 中第一阶段完成后（`AnalysisTask.py:116` 附近）插入：

```python
if getattr(self.config, "extract_debug_dump_switch", False):
    dump_path = os.path.join(self.config.label_output_path, "debug_stage1_dump.json")
    with open(dump_path, "w", encoding="utf-8") as f:
        json.dump({
            "created_at": datetime.now().isoformat(),
            "config_digest": {"model": self.config.model,
                              "source_language": self.config.source_language,
                              "target_language": self.config.target_language,
                              "token_limit": extract_task_token_limit},
            "chunks_source_text": ["\n".join(i.get("source_text","") for i in c) for c in chunks],
            "first_stage_results": first_stage_results,
        }, f, ensure_ascii=False, indent=2)
```

要点：

- `chunks_source_text` 必须一起保存——1.2 的频率统计需要全文，重放时不能依赖重新打开项目。
- 配置摘要用于重放时提示"快照与当前配置不匹配"（提示而非阻断）。
- 开关 `extract_debug_dump_switch` 不进 UI，只认配置文件里的手写键，避免普通用户误开。

### 8.2 快照重放（两条路径，都要）

**路径 A：离线脚本（主力调试工具）**，新建 `Tests/replay_stage2.py`：

```
用法: python Tests/replay_stage2.py debug_stage1_dump.json [--dry-run] [--cluster-only]
```

- `--cluster-only`：只跑本地预处理（归一化/频率/成簇/折叠），打印簇结构、折叠日志、频率表，**零 API 调用**。开发 ExtractionClustering 时的最快反馈回路。
- `--dry-run`：额外构建二阶段 prompt 并打印，仍不发请求，用于人工检查 prompt/few-shot 形态。
- 默认：完整执行二阶段（构造 `AnalysisTask`，注入 dump 的 `first_stage_results` 跳过第一阶段，调用真实 `_prepare_reduction_batches` + `_run_second_stage` + `_finalize_results`），把最终 `final_data` 写到 stdout/文件，不碰 CacheManager、不落盘项目缓存。
- 实现前提：脚本可以直接构造 `AnalysisTask(cache_manager=None, ...)` 并手动 `config.initialize("extract")`；`_finalize_results` 及之前的链路不触碰 cache_manager（当前代码满足，重构时保持这一性质——**这是可重放性的架构约束**，写进代码注释）。

**路径 B：主流程内重放**：配置键 `extract_debug_replay_path` 非空时，`run()` 跳过第一阶段，从快照加载 `first_stage_results` 和全文。用于最终在真实 UI/落盘链路上验证一次。

### 8.3 对重构的结构性要求

为了让路径 A 成立，阶段 2/3 的新代码遵守：

- `_prepare_reduction_batches` 的输入只有 `first_stage_results` + 全文文本 + config，不隐式读取 cache_manager（全文作为显式参数传入，而不是在函数内部再去 `generate_analysis_source_chunks`）。
- 本地预处理（ExtractionClustering）本来就是纯函数，天然可重放。
- 提交切分上，8.1/8.2 的 dump + replay 脚本放在**步骤 1 之后、步骤 2 之前**落地（见第 7 节顺序调整）：先有调试工具，再做有风险的改造。

### 8.4 顺带收益

- `--cluster-only` 的输出可以直接固化为 1.5 快照测试的 fixture 生成器。
- dump 文件本身就是 bug 报告载体：用户遇到裁决问题时可以只发 dump，不用发整个项目。

---

## 9. 明确不做的事（防 scope creep）

- 按频率阈值删除低频词条——低频专名恰恰最需要词表保护，且词条不匹配即不注入、留着零成本；只有 independent_count==0 的确定性折叠，没有任何"低频删除"规则（GalTransl/Name Detector 的阈值删除模式明确不采纳）。
- alias-only 数据模型（BookLLM 模式）——短形式只做 alias 会导致翻译阶段丢失匹配，与原则 1 冲突。
- 指代消解（`先生` 单独出现指谁）——超出词表范畴。
- 音变昵称（`さっちゃん`）的自动关联——留给 AI 在簇内自行判断，不做本地规则。
- Aho-Corasick 等包含判断优化——量级不需要。
- 翻译阶段的最长匹配/重叠匹配策略（LunaTranslator 的 token 保护、Context-aware l10n 的稳定 ID）——词表消费端的事，另开任务。
- 本地 NLP 候选发现（SudachiPy/片假名正则，GalTransl 模式）——有价值的第一阶段补充，但独立于本次成簇重构，另开任务。
- JSON 截断续传恢复（BookLLM 模式）——现有整批重试 + 拆批已够，量级不同。
- 词条来源/锁定/优先级体系（translation_source、manual locked，Supervertaler/AI Novel Translation 模式）——值得做，但属于词表管理功能，另开任务。
