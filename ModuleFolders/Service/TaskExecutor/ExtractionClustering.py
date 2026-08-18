"""
抽取阶段本地预处理（POC）：
归一化 -> 敬称剥离 -> 包含关系连通分量成簇 -> 真实频率统计 -> 零独立出现折叠

全部为纯函数/纯数据类，不依赖 AnalysisTask/Qt/LLM，可独立单测与离线重放。
详见 EXTRACTION_REFACTOR_PLAN.md 第 1 节。
"""

from dataclasses import dataclass, field


# 只白名单式转换分隔符与空格，不做 NFKC（避免破坏 ％s、＜b＞ 等禁翻 marker）
_NORMALIZE_TABLE = str.maketrans({
    "･": "・",
    "·": "・",
    "　": " ",
})

# 敬称表：按源语言组织，长的在前，只做单层剥离
HONORIFIC_SUFFIXES = {
    "japanese": [
        "お嬢様", "陛下", "殿下", "閣下", "先生", "先輩",
        "さん", "さま", "様", "くん", "君", "ちゃん", "たん",
        "殿", "どの", "氏", "嬢", "卿", "夫人",
    ],
    "korean": ["씨", "님", "군", "양"],
}

MAX_UNIT_SIZE = 30  # 发送单元成员上限（语义簇无上限）
MIN_CONTAIN_LEN = 2  # 包含边最小长度门槛，单字词不建包含边


def normalize_source(source: str) -> str:
    """仅用于分组比较的归一化，不修改词条实际存储的 source。"""
    return str(source or "").translate(_NORMALIZE_TABLE).strip()


def strip_honorific(source: str, known_sources: set, suffixes: list) -> tuple:
    """base-must-exist 剥离。返回 (base, honorific) 或 (None, None)。"""
    for suffix in suffixes:
        if source.endswith(suffix):
            base = source[: -len(suffix)]
            if len(base) >= MIN_CONTAIN_LEN and base in known_sources:
                return base, suffix
    return None, None


class _UnionFind:
    def __init__(self, items):
        self.parent = {item: item for item in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


@dataclass
class Cluster:
    """一个语义簇（连通分量）。members 已剔除被折叠词。"""
    members: list = field(default_factory=list)          # 原始 source，按长度降序
    honorific_edges: dict = field(default_factory=dict)  # variant -> (base, honorific)
    contain_edges: list = field(default_factory=list)    # (short, long, shared_fragment)
    collapse_targets: dict = field(default_factory=dict) # short -> [(long, 覆盖次数)]
    frequencies: dict = field(default_factory=dict)      # source -> (total, independent)


def _find_occurrences(needle: str, haystack: str) -> list:
    """返回 needle 在 haystack 中所有出现区间 [start, end)。"""
    spans, start = [], 0
    while True:
        idx = haystack.find(needle, start)
        if idx < 0:
            break
        spans.append((idx, idx + len(needle)))
        start = idx + 1
    return spans


def _covered_count(short_spans: list, long_spans: list) -> int:
    """short 的出现中被任一 long 区间完全覆盖的次数。"""
    covered = 0
    for s, e in short_spans:
        if any(ls <= s and e <= le for ls, le in long_spans):
            covered += 1
    return covered


def build_clusters(sources: list, source_language: str = "", full_text: str = "") -> list:
    """
    输入：第一阶段去重后的原始 source 列表 + 全文原文。
    输出：Cluster 列表（含折叠与频率信息）。
    """
    originals = [s for s in dict.fromkeys(str(s).strip() for s in sources) if s]
    norm_map = {}  # normalized -> [original, ...]
    for s in originals:
        norm_map.setdefault(normalize_source(s), []).append(s)
    norm_keys = list(norm_map.keys())

    suffixes = HONORIFIC_SUFFIXES.get(str(source_language or "").lower(), [])
    known = set(norm_keys)

    uf = _UnionFind(norm_keys)
    honorific_edges = {}   # norm variant -> (norm base, honorific)
    contain_edges = []     # (norm short, norm long)

    # 敬称边
    for nk in norm_keys:
        base, honorific = strip_honorific(nk, known, suffixes)
        if base:
            honorific_edges[nk] = (base, honorific)
            uf.union(nk, base)

    # 包含边（单字词不建边，防 凯/凯尔 误连）
    sorted_norm = sorted(norm_keys, key=lambda s: (-len(s), s))
    for i, long_k in enumerate(sorted_norm):
        for short_k in sorted_norm[i + 1:]:
            if len(short_k) < MIN_CONTAIN_LEN:
                continue
            if short_k != long_k and short_k in long_k:
                contain_edges.append((short_k, long_k))
                uf.union(short_k, long_k)

    # 连通分量
    components = {}
    for nk in norm_keys:
        components.setdefault(uf.find(nk), []).append(nk)

    norm_text = normalize_source(full_text) if full_text else ""

    clusters = []
    for comp in components.values():
        comp_sorted = sorted(comp, key=lambda s: (-len(s), s))
        cluster = Cluster()

        # 频率：total 全文子串计数；independent = 不被簇内更长词覆盖的次数
        spans_by_key = {}
        if norm_text:
            for nk in comp_sorted:
                spans_by_key[nk] = _find_occurrences(nk, norm_text)
        for nk in comp_sorted:
            spans = spans_by_key.get(nk, [])
            total = len(spans)
            longer_spans = []
            for other in comp_sorted:
                if len(other) > len(nk) and nk in other:
                    longer_spans.extend(spans_by_key.get(other, []))
            covered = _covered_count(spans, longer_spans) if longer_spans else 0
            independent = total - covered
            for original in norm_map[nk]:
                cluster.frequencies[original] = (total, independent)

        # 零独立出现折叠（多父：记录全部覆盖者及覆盖次数）
        collapsed_norm = set()
        for nk in comp_sorted:
            total, independent = cluster.frequencies.get(norm_map[nk][0], (0, 0))
            if total > 0 and independent == 0 and nk not in honorific_edges:
                covers = []
                for other in comp_sorted:
                    if len(other) > len(nk) and nk in other:
                        n_cover = _covered_count(spans_by_key.get(nk, []), spans_by_key.get(other, []))
                        if n_cover > 0:
                            covers.append((norm_map[other][0], n_cover))
                if covers:
                    covers.sort(key=lambda x: -x[1])
                    for original in norm_map[nk]:
                        cluster.collapse_targets[original] = covers
                    collapsed_norm.add(nk)

        # members（保留原始形式，剔除被折叠词）
        for nk in comp_sorted:
            if nk in collapsed_norm:
                continue
            cluster.members.extend(norm_map[nk])

        for variant_nk, (base_nk, honorific) in honorific_edges.items():
            if variant_nk in comp:
                for original in norm_map[variant_nk]:
                    cluster.honorific_edges[original] = (norm_map[base_nk][0], honorific)

        member_norms = {normalize_source(m) for m in cluster.members}
        for short_k, long_k in contain_edges:
            if short_k in member_norms and long_k in member_norms and short_k in comp:
                cluster.contain_edges.append(
                    (norm_map[short_k][0], norm_map[long_k][0], norm_map[short_k][0])
                )

        clusters.append(cluster)

    return clusters


def split_into_units(cluster: Cluster) -> list:
    """语义簇 -> 发送单元（成员列表）。超过 MAX_UNIT_SIZE 时拆分（POC：顺序切块）。"""
    members = cluster.members
    if len(members) <= MAX_UNIT_SIZE:
        return [members]
    return [members[i:i + MAX_UNIT_SIZE] for i in range(0, len(members), MAX_UNIT_SIZE)]


def aggregate_candidates(candidates: list) -> list:
    """按 (type, recommended_translation) 投票聚合，压缩 token 并显式化候选稳定度。"""
    buckets = {}
    for cand in candidates:
        key = (str(cand.get("type", "")).strip(),
               str(cand.get("recommended_translation", "")).strip())
        bucket = buckets.setdefault(key, {
            "type": key[0], "recommended_translation": key[1],
            "vote_count": 0, "gender": "", "category_path": "", "note": "",
            "candidate_source": str(cand.get("candidate_source", "")).strip(),
        })
        bucket["vote_count"] += 1
        for f in ("gender", "category_path"):
            v = str(cand.get(f, "")).strip()
            if v and v != "其他" and not bucket[f]:
                bucket[f] = v
        note = str(cand.get("note", "")).strip()
        if len(note) > len(bucket["note"]):
            bucket["note"] = note
    return sorted(buckets.values(), key=lambda b: -b["vote_count"])
