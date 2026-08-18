import concurrent.futures
import os
import re
from datetime import datetime

import rapidjson as json

from ModuleFolders.Base.Base import Base
from ModuleFolders.Config.Config import ConfigMixin
from ModuleFolders.Log.Log import LogMixin
from ModuleFolders.Infrastructure.LLMRequester.LLMRequester import LLMRequester
from ModuleFolders.Infrastructure.RequestLimiter.RequestLimiter import RequestLimiter
from ModuleFolders.Infrastructure.TaskConfig.TaskConfig import TaskConfig
from ModuleFolders.Infrastructure.Tokener.Tokener import Tokener
from ModuleFolders.Service.TaskExecutor import TranslatorUtil
from ModuleFolders.Service.TaskExecutor import ExtractionClustering


class AnalysisTask(ConfigMixin, LogMixin, Base):
    """
    数据流概览 (总-分-分 结构)：
    [总] run(): 统筹调度整个分析任务。
    [分] 第一阶段 (Stage 1): 按 token 切分原文，独立请求 AI 抽取 candidates。
    [分] 第二阶段 (Stage 2): 按 source 聚合第一阶段的结果，交由 AI 裁决合并。
    [分] 最终阶段 (Finalize): 主线程收口结果，程序兜底遗漏项，清洗禁翻项并落盘。
    """
    
    REDUCE_BATCH_TOKEN_LIMIT = 10000
    MAX_REQUEST_ATTEMPTS = 2
    COMMON_PUNCTUATION_CHARS = set(
        ".,!?;:'\"-_=+~`^…—、，。！？；：‘’“”()（）[]【】{}《》<>「」『』〈〉〔〕﹝﹞·•/\\|"
    )
    
    def __init__(self, cache_manager, set_active_executor, clear_active_executor) -> None:
        super().__init__()
        self.cache_manager = cache_manager
        self._set_active_executor = set_active_executor
        self._clear_active_executor = clear_active_executor
        self.config = TaskConfig()
        self.request_limiter = RequestLimiter()
        self.grouped_stage_two_inputs = {} # 第二阶段的输入结构：{主source: {source, merged_sources, candidates}}
        self.grouped_stage_two_source_aliases = {} # 第二阶段的 source 别名映射：{所有source: 主source}
        # --- 成簇模式（POC）状态 ---
        self._analysis_full_text = ""      # 全文原文，用于真实频率统计
        self.source_frequencies = {}       # source -> (total_count, independent_count)
        self.honorific_variant_map = {}    # 敬称变体 -> (base, honorific)
        self.collapse_log = []             # 零独立出现折叠记录 [{source, targets, reason}]


    # ========================================================================
    # 1. 总流程控制 (总)
    # ========================================================================

    def run(self) -> None:
        """主入口统筹：准备 -> 第一阶段并发 -> 第二阶段聚合与并发 -> 最终兜底合并 -> 落盘"""
        try:
            # --- [准备阶段] ---
            Base.work_status = Base.STATUS.ANALYSIS_TASK
            self.info("开始执行文本分析任务 ...")
            self._emit_progress_update(
                "prepare",
                self.tra("准备中"),
                0,
                0,
                self.tra("正在初始化分析任务..."),
                self.tra("加载配置与限流设置。"),
            )
            self.config.initialize("extract")
            self.config.prepare_for_active_platform("extract")
            self.request_limiter.set_limit(self.config.rpm_limit)
            self.info(
                "分析配置: 模型 - {0}, 并发线程 - {1}, RPM - {2}".format(
                    self.config.model,
                    self.config.actual_thread_counts,
                    self.config.rpm_limit,
                )
            )

            # 生成分析用文本片段：按 token 切分，确保每个分块都在模型处理能力范围内，同时保持文本的完整性和上下文连贯。
            self._emit_progress_update(
                "prepare",
                self.tra("准备中"),
                0,
                0,
                self.tra("正在生成分析用文本片段..."),
                self.tra("按 token 切分项目原文。"),
            )
            extract_task_token_limit = max(1, int(getattr(self.config, "extract_task_token_limit", 10000)))
            chunks = self.cache_manager.generate_analysis_source_chunks("token", extract_task_token_limit)
            self._analysis_full_text = "\n".join(
                item.get("source_text", "") for chunk in chunks for item in chunk
            )
            self.info(f"分析文本切分完成，共生成 {len(chunks)} 个分块。")

            # --- [第一阶段] ---
            self._emit_progress_update(
                "stage1",
                self.tra("第一阶段"),
                0,
                len(chunks),
                self.tra("开始执行第一阶段分析任务..."),
                self.tra("共 {0} 个分块。").format(len(chunks)),
            )
            self.info(f"开始执行第一阶段提取，共 {len(chunks)} 个分块。")
            first_stage_results = []
            replay_results = self._try_load_stage1_replay()
            if replay_results is not None:
                first_stage_results = replay_results
                self.info(f"[调试] 已从快照重放第一阶段结果，共 {len(first_stage_results)} 个分块结果，跳过第一阶段请求。")
            else:
                executor_stage1 = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.actual_thread_counts, thread_name_prefix="analysis_stage1")
                self._set_active_executor(executor_stage1)
                try:
                    futures_stage1 = [executor_stage1.submit(self._run_first_stage, chunk) for chunk in chunks]
                    for i, future in enumerate(concurrent.futures.as_completed(futures_stage1), 1):
                        if Base.work_status == Base.STATUS.STOPING: break
                        if result := future.result(): first_stage_results.append(result)
                        self._emit_progress_update(
                            "stage1",
                            self.tra("第一阶段"),
                            i,
                            len(chunks),
                            self.tra("第一阶段提取中..."),
                            self.tra("已完成 {0} / {1} 个分块。").format(i, len(chunks)),
                        )
                finally:
                    executor_stage1.shutdown(wait=True, cancel_futures=Base.work_status == Base.STATUS.STOPING)
                    self._clear_active_executor(executor_stage1)

            if Base.work_status == Base.STATUS.STOPING: return self._handle_stop()
            self.info(f"第一阶段提取完成，成功收集 {len(first_stage_results)} 个分块结果。")
            self._maybe_dump_stage1(first_stage_results)

            # --- [第二阶段] ---
            reduction_batches = self._prepare_reduction_batches(first_stage_results) # 准备第二阶段的批次：将第一阶段的结果按 source 聚合，短词挂靠长词，形成待裁决的候选组。
            self._emit_progress_update(
                "stage2",
                self.tra("第二阶段"),
                0,
                len(reduction_batches),
                self.tra("开始执行第二阶段分析任务..."),
                self.tra("共 {0} 个合并批次。").format(len(reduction_batches)),
            )
            self.info(f"开始执行第二阶段合并，共 {len(reduction_batches)} 个批次。")
            second_stage_results = []
            if reduction_batches:
                executor_stage2 = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.actual_thread_counts, thread_name_prefix="analysis_stage2")
                self._set_active_executor(executor_stage2)
                try:
                    futures_stage2 = [executor_stage2.submit(self._run_second_stage, batch) for batch in reduction_batches]
                    for i, future in enumerate(concurrent.futures.as_completed(futures_stage2), 1):
                        if Base.work_status == Base.STATUS.STOPING: break
                        if result := future.result(): second_stage_results.append(result)
                        self._emit_progress_update(
                            "stage2",
                            self.tra("第二阶段"),
                            i,
                            len(reduction_batches),
                            self.tra("第二阶段合并中..."),
                            self.tra("已完成 {0} / {1} 个合并批次。").format(i, len(reduction_batches)),
                        )
                finally:
                    executor_stage2.shutdown(wait=True, cancel_futures=Base.work_status == Base.STATUS.STOPING)
                    self._clear_active_executor(executor_stage2)
                self.info(f"第二阶段合并完成，成功收集 {len(second_stage_results)} 个批次结果。")
            else:
                self.info("第二阶段没有可合并候选，已跳过 AI 裁决。")

            if Base.work_status == Base.STATUS.STOPING: return self._handle_stop()

            # --- [最终阶段] ---
            self._emit_progress_update(
                "finalize",
                self.tra("结果整合"),
                0,
                1,
                self.tra("正在整合最终分析结果..."),
                self.tra("执行本地兜底与禁翻清洗。"),
            )
            self.info("开始汇总最终分析结果并写回缓存 ...")
            final_data = self._finalize_results(first_stage_results, second_stage_results)
            if not getattr(self.config, "auto_extract_non_translate_switch", False):
                final_data["non_translate"] = []
            self._emit_progress_update(
                "finalize",
                self.tra("结果整合"),
                1,
                1,
                self.tra("分析结果已生成..."),
                self.tra("正在写回项目缓存。"),
            )

            analysis_data = {
                "status": "success",
                "last_run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "characters": final_data.get("characters", []),
                "terms": final_data.get("terms", []),
                "non_translate": final_data.get("non_translate", []),
                "stats": {
                    "character_count": len(final_data.get("characters", [])),
                    "term_count": len(final_data.get("terms", [])),
                    "non_translate_count": len(final_data.get("non_translate", [])),
                },
            }
            analysis_data["stats"]["total_hits"] = sum(analysis_data["stats"].values())
            
            self.cache_manager.set_analysis_data(analysis_data)
            self.cache_manager.require_save_to_file()
            self.info(
                "文本分析完成。角色 {0} 个，术语 {1} 个，非翻译项 {2} 个，总计 {3} 个。".format(
                    analysis_data["stats"]["character_count"],
                    analysis_data["stats"]["term_count"],
                    analysis_data["stats"]["non_translate_count"],
                    analysis_data["stats"]["total_hits"],
                )
            )
            Base.work_status = Base.STATUS.IDLE
            self.emit(
                Base.EVENT.ANALYSIS_TASK_DONE,
                {
                    "status": "success",
                    "analysis_data": analysis_data,
                    "message": self.tra("全文分析完成。"),
                },
            )

        except Exception as error:
            self.error(f"分析任务执行失败: {error}", error)
            Base.work_status = Base.STATUS.IDLE
            self.emit(Base.EVENT.ANALYSIS_TASK_DONE, {"status": "error", "analysis_data": None, "message": str(error)})

    def _handle_stop(self) -> None:
        Base.work_status = Base.STATUS.TASKSTOPPED
        self.info("文本分析任务已停止。")
        self.emit(
            Base.EVENT.ANALYSIS_TASK_DONE,
            {
                "status": "stopped",
                "analysis_data": None,
                "message": self.tra("分析任务已停止。"),
            },
        )


    # ========================================================================
    # 2. 第一阶段：独立文本提取 (分)
    # ========================================================================

    def _run_first_stage(self, chunk: list) -> dict:
        """执行单分块的角色/术语/禁翻提取"""
        try:
            # 将 chunk 中的 source_text 合并为一个字符串
            source_text = "\n".join(item.get("source_text", "") for item in chunk)

            # 构建该chunk的系统提示和消息列表
            system_prompt, messages = self._build_first_stage_prompt(source_text)
            
            # 执行请求并返回结果，若失败则返回空结构
            result = self._execute_analysis_request(
                stage_label="第一阶段提取",
                system_prompt=system_prompt,
                messages=messages,
                required_fields=("characters", "terms", "non_translate")
            )
            return result or {"characters": [], "terms": [], "non_translate": []}
            
        except Exception as error:
            self.error(f"第一阶段提取失败: {error}")
            return {"characters": [], "terms": [], "non_translate": []}

    def _get_recommended_translation_language_requirement(self) -> str:
        """构建 recommended_translation 的目标语言约束说明。"""
        target_language = str(getattr(self.config, "target_language", "") or "").strip()
        display_name = TranslatorUtil.pair.get(target_language, "")
        if display_name:
            return f"角色与术语的译名/分类/备注，不翻译项的分类/备注都必须写成{display_name}。"

        return "角色与术语的译名/分类/备注，不翻译项的分类/备注都必须跟随当前译文语言设置。"

    def _build_first_stage_prompt(self, source_text: str) -> tuple[str, list[dict]]:
            """第一阶段：独立文本提取（优化版提示词）"""
            system_prompt = (
                "你是一个专业的游戏与本地化文本分析专家。你的唯一任务是从给定的文本中提取出：角色名、专有名词（术语）以及不需要翻译的代码/标记。\n"
                "【严格执行以下规则】\n"
                "1. 原样提取：提取的 `source` 必须与原文一字不差，绝对不要修改大小写或标点。\n"
                "2. 拒绝脑补：只提取文本中实际出现的实体，不要联想或创造。\n"
                "3. 宁缺毋滥：对于普通词汇（如“苹果”、“跑”、“明天”），不要提取。如果没有值得提取的内容，返回空列表。\n"
                "4. 分类规范：\n"
                "   - characters(角色): 文本中出现的具体人物、怪物、神明等名字。gender 建议分类: 男性/女性/其他。\n"
                "   - terms(术语): 身份称谓、地名、组织、物品名、技能名、种族名、独特概念等。category_path 建议分类: 身份/物品/组织/地名/技能/种族/其他。\n"
                "   - non_translate(不翻译项): 必须保留的机器代码，如 HTML标签(<b>)、占位符(%s)、变量({{name}})。category 建议分类: 标签/变量/占位符/标记符/转义控制符/资源标识/数值公式/其他。\n"
                "【输出格式】\n"
                "必须输出合法的 JSON 代码块，严格遵守以下结构：\n"
                "```json\n"
                "{\n"
                "  \"characters\": [{\"source\": \"原文\", \"recommended_translation\": \"推荐译名\", \"gender\": \"\", \"note\": \"\"}],\n"
                "  \"terms\": [{\"source\": \"原文\", \"recommended_translation\": \"推荐译名\", \"category_path\": \"\", \"note\": \"\"}],\n"
                "  \"non_translate\": [{\"marker\": \"代码或标记\", \"category\": \"\", \"note\": \"\"}]\n"
                "}\n"
                "```"
            )
            
            # 优化 Few-Shot：包含更丰富的混合场景，注意 {{}} 是转义 Python 的 f-string 占位符
            fake_user = (
                "请分析以下文本并提取信息，角色与术语的译名/分类/备注，不翻译项的分类/备注都必须写成简体中文。\n"
                "---\n"
                "露娜小姐：请携带[圣剑]前往星门集合。\n"
                "精灵族战士即将施放月光斩。\n"
                "系统提示：欢迎回来，{{player_name}}！<br>请注意<color=red>HP</color>的变化。\n"
                "---\n"
                "请输出 JSON 提取结果。"
            )
            fake_assistant = (
                "```json\n"
                "{\n"
                "  \"characters\": [\n"
                "    {\"source\": \"露娜小姐\", \"recommended_translation\": \"露娜小姐\", \"gender\": \"女性\", \"note\": \"NPC称呼\"}\n"
                "  ],\n"
                "  \"terms\": [\n"
                "    {\"source\": \"圣剑\", \"recommended_translation\": \"圣剑\", \"category_path\": \"物品\", \"note\": \"武器名称\"},\n"
                "    {\"source\": \"星门\", \"recommended_translation\": \"星门\", \"category_path\": \"地名\", \"note\": \"地点\"},\n"
                "    {\"source\": \"月光斩\", \"recommended_translation\": \"月光斩\", \"category_path\": \"技能\", \"note\": \"招式名称\"},\n"
                "    {\"source\": \"精灵族\", \"recommended_translation\": \"精灵族\", \"category_path\": \"种族\", \"note\": \"种族名称\"}\n"
                "  ],\n"
                "  \"non_translate\": [\n"
                "    {\"marker\": \"{{player_name}}\", \"category\": \"变量\", \"note\": \"玩家名变量\"},\n"
                "    {\"marker\": \"<br>\", \"category\": \"标签\", \"note\": \"换行符\"},\n"
                "    {\"marker\": \"<color=red>\", \"category\": \"标签\", \"note\": \"颜色富文本标签\"},\n"
                "    {\"marker\": \"</color>\", \"category\": \"标签\", \"note\": \"颜色富文本标签闭合\"}\n"
                "  ]\n"
                "}\n"
                "```"
            )
            
            language_requirement = self._get_recommended_translation_language_requirement()
            user_prompt = (
                f"请分析以下文本并提取信息，{language_requirement}\n"
                f"---\n{source_text}\n---\n"
                "请输出 JSON 提取结果。"
            )
            messages = [
                {"role": "user", "content": fake_user},
                {"role": "assistant", "content": fake_assistant},
                {"role": "user", "content": user_prompt},
            ]
            return system_prompt, messages


    # ========================================================================
    # 3. 第二阶段：候选归并与 AI 裁决 (分)
    # ========================================================================

    def _run_second_stage(self, batch: list) -> dict:
        """对聚合后的长短变体候选组，让 AI 裁决去留和分类"""
        if self._is_cluster_mode():
            return self._run_second_stage_cluster(batch)
        try:
            system_prompt, messages = self._build_second_stage_prompt(batch)
            
            # 第二阶段特有业务校验：输入 batch 不为空时，输出不能全为空
            def stage_two_validator(parsed: dict):
                if batch and not parsed.get("characters") and not parsed.get("terms"):
                    return False, "未返回任何合并裁决结果"
                return True, ""

            # 复用通用流水线，注入第二阶段专属校验规则
            result = self._execute_analysis_request(
                stage_label="第二阶段合并",
                system_prompt=system_prompt,
                messages=messages,
                required_fields=("characters", "terms"),
                custom_validator=stage_two_validator
            )
            return result or {"characters": [], "terms": []}
            
        except Exception as error:
            self.error(f"第二阶段合并失败: {error}")
            return {"characters": [], "terms": []}

    def _run_second_stage_cluster(self, batch: list) -> dict:
        """成簇模式：AI 一次完成类型裁决 + 簇内共享片段译名统一，严格 entry_id/source 校验。"""
        try:
            expected = {
                entry["entry_id"]: entry["source"]
                for unit in batch for entry in unit.get("entries", [])
            }
            multi_member_ids = {
                entry["entry_id"]
                for unit in batch if len(unit.get("entries", [])) > 1
                for entry in unit["entries"]
            }

            def cluster_validator(parsed: dict):
                returned = []
                for key in ("characters", "terms", "discarded"):
                    for item in parsed.get(key, []) or []:
                        if not isinstance(item, dict):
                            return False, f"{key} 中存在非对象内容"
                        returned.append((key, item))
                returned_ids = [item.get("entry_id") for _k, item in returned]
                if len(returned_ids) != len(set(returned_ids)):
                    return False, "entry_id 出现多次"
                if set(returned_ids) != set(expected.keys()):
                    missing = set(expected.keys()) - set(returned_ids)
                    extra = set(returned_ids) - set(expected.keys())
                    return False, f"entry_id 集合与输入不一致 (缺失 {sorted(missing)[:5]}, 多余 {sorted(extra)[:5]})"
                for key, item in returned:
                    if key == "discarded" and item.get("entry_id") in multi_member_ids:
                        return False, "多成员单元不允许丢弃成员"
                    if key != "discarded" and not str(item.get("recommended_translation", "")).strip():
                        return False, "recommended_translation 为空"
                # source 漂移自动修正：entry_id 正确即接受，source 以输入为准
                for _key, item in returned:
                    item["source"] = expected[item["entry_id"]]
                return True, ""

            system_prompt, messages = self._build_second_stage_cluster_prompt(batch)
            result = self._execute_analysis_request(
                stage_label="第二阶段成簇裁决",
                system_prompt=system_prompt,
                messages=messages,
                required_fields=("characters", "terms", "discarded"),
                custom_validator=cluster_validator,
            )
            return result or {"characters": [], "terms": [], "discarded": []}

        except Exception as error:
            self.error(f"第二阶段成簇裁决失败: {error}")
            return {"characters": [], "terms": [], "discarded": []}

    def _build_second_stage_cluster_prompt(self, batch: list) -> tuple[str, list[dict]]:
        """成簇模式二阶段提示词：类型裁决 + 译名统一一次完成，保留所有 entry。"""
        system_prompt = (
            "你是一个本地化术语库规范化专家。你将收到多个“名称簇（cluster）”，每个簇内的名称存在包含关系（如全名与姓氏、名字与称号）。\n"
            "你的任务：对每个 entry 裁决它属于“角色(characters)”还是“术语(terms)”，并统一簇内共享名称片段的译法。\n"
            "【严格执行以下规则】\n"
            "1. 完整返回：每个 entry 的 entry_id 和 source 必须在 characters/terms/discarded 三处恰好出现一次，不得新增、修改或遗漏。\n"
            "2. 唯一归属：同一 entry 只能是角色或术语之一。\n"
            "3. 译名一致：relations 标注了名称间的包含关系和共享片段（shared_fragment），共享片段在各词条译名中必须使用相同译法。姓氏与全名不是同一人，仍需分别保留词条，绝不能因为一个名称包含另一个就省略词条。\n"
            "4. 证据使用：count 是原文真实出现次数，independent_count 是不被更长名称覆盖的独立出现次数，vote_count 是该译名被多少个文本分块独立提出。统一译法冲突时，优先采用 independent_count 和 vote_count 更高的既有译名。\n"
            "5. honorific_variants 仅供性别/身份判断参考，不需要为其输出结果。\n"
            "6. 谨慎丢弃：只有确定是无价值普通词汇（如“今天”“然后”）才放入 discarded。人名、地名、称号、姓氏一律不丢弃。\n"
            "【输出格式】必须输出合法的 JSON 代码块：\n"
            "```json\n"
            "{\n"
            "  \"characters\": [{\"entry_id\": 101, \"source\": \"原样返回\", \"recommended_translation\": \"译名\", \"gender\": \"\", \"note\": \"\"}],\n"
            "  \"terms\": [{\"entry_id\": 102, \"source\": \"原样返回\", \"recommended_translation\": \"译名\", \"category_path\": \"\", \"note\": \"\"}],\n"
            "  \"discarded\": [{\"entry_id\": 103, \"source\": \"原样返回\"}]\n"
            "}\n"
            "```"
        )

        # Few-Shot：输入 3 个 entry 必须输出 3 条结果（防止模型退回旧合并行为的关键）
        sample_batch = [{
            "cluster_id": 0,
            "entries": [
                {"entry_id": 1, "source": "ヴェーネ・アンスバッハ", "count": 5, "independent_count": 5,
                 "candidates": [{"type": "character", "recommended_translation": "维内·安斯巴赫", "vote_count": 4, "gender": "女性", "note": "子爵家长女"}]},
                {"entry_id": 2, "source": "アンスバッハ", "count": 50, "independent_count": 10,
                 "candidates": [{"type": "term", "recommended_translation": "安斯巴赫", "vote_count": 6, "category_path": "身份", "note": "贵族姓氏"}]},
                {"entry_id": 3, "source": "ヴェーネ", "count": 200, "independent_count": 195,
                 "candidates": [{"type": "character", "recommended_translation": "薇涅", "vote_count": 8, "gender": "女性", "note": "主角"}]},
            ],
            "relations": [
                {"type": "contains", "short": 2, "long": 1, "shared_fragment": "アンスバッハ"},
                {"type": "contains", "short": 3, "long": 1, "shared_fragment": "ヴェーネ"},
            ],
        }, {
            "cluster_id": 1,
            "entries": [
                {"entry_id": 4, "source": "それでは", "count": 30, "independent_count": 30,
                 "candidates": [{"type": "term", "recommended_translation": "那么", "vote_count": 1, "category_path": "其他", "note": ""}]},
            ],
            "relations": [],
        }]
        fake_user = (
            "请分析以下名称簇并完成裁决与译名统一，译名/分类/备注必须写成简体中文。\n"
            f"---\n{json.dumps(sample_batch, ensure_ascii=False)}\n---\n"
            "请输出 JSON 结果。"
        )
        fake_assistant = (
            "```json\n"
            "{\n"
            "  \"characters\": [\n"
            "    {\"entry_id\": 3, \"source\": \"ヴェーネ\", \"recommended_translation\": \"薇涅\", \"gender\": \"女性\", \"note\": \"主角。独立出现195次，票数最高，以此为准\"},\n"
            "    {\"entry_id\": 1, \"source\": \"ヴェーネ・アンスバッハ\", \"recommended_translation\": \"薇涅·安斯巴赫\", \"gender\": \"女性\", \"note\": \"子爵家长女，全名。名字部分与ヴェーネ统一\"}\n"
            "  ],\n"
            "  \"terms\": [\n"
            "    {\"entry_id\": 2, \"source\": \"アンスバッハ\", \"recommended_translation\": \"安斯巴赫\", \"category_path\": \"身份\", \"note\": \"贵族姓氏，多名角色共用\"}\n"
            "  ],\n"
            "  \"discarded\": [\n"
            "    {\"entry_id\": 4, \"source\": \"それでは\"}\n"
            "  ]\n"
            "}\n"
            "```"
        )
        language_requirement = self._get_recommended_translation_language_requirement()
        user_prompt = (
            f"请分析以下名称簇并完成裁决与译名统一，{language_requirement}\n"
            f"---\n{json.dumps(batch, ensure_ascii=False)}\n---\n"
            "请输出 JSON 结果。"
        )
        messages = [
            {"role": "user", "content": fake_user},
            {"role": "assistant", "content": fake_assistant},
            {"role": "user", "content": user_prompt},
        ]
        return system_prompt, messages

    def _prepare_reduction_batches(self, first_stage_results: list) -> list:
        """组装第二阶段所需的候选组批次：短词挂靠长词"""
        raw_grouped_inputs = {}
        for result in first_stage_results:
            for row in result.get("characters", []):
                source = str(row.get("source", "")).strip()
                if not source: continue
                raw_grouped_inputs.setdefault(source, {"source": source, "merged_sources": [source], "candidates": []})
                raw_grouped_inputs[source]["candidates"].append({
                    "candidate_source": source, "type": "character", "recommended_translation": str(row.get("recommended_translation", "")).strip(),
                    "gender": str(row.get("gender", "")).strip(), "category_path": "", "note": str(row.get("note", "")).strip(),
                })
            for row in result.get("terms", []):
                source = str(row.get("source", "")).strip()
                if not source: continue
                raw_grouped_inputs.setdefault(source, {"source": source, "merged_sources": [source], "candidates": []})
                raw_grouped_inputs[source]["candidates"].append({
                    "candidate_source": source, "type": "term", "recommended_translation": str(row.get("recommended_translation", "")).strip(),
                    "gender": "", "category_path": str(row.get("category_path", "")).strip(), "note": str(row.get("note", "")).strip(),
                })

        if self._is_cluster_mode():
            return self._prepare_reduction_batches_cluster(raw_grouped_inputs)

        sorted_sources = sorted(raw_grouped_inputs.keys(), key=lambda s: (-len(s), s))
        grouped_inputs, source_aliases, consumed_sources = {}, {}, set()

        for source in sorted_sources:
            if source in consumed_sources: continue
            merged_group = {"source": source, "merged_sources": [source], "candidates": list(raw_grouped_inputs[source].get("candidates", []))}
            grouped_inputs[source] = merged_group
            source_aliases[source] = source
            consumed_sources.add(source)

            for other_source in sorted_sources:
                if other_source in consumed_sources or other_source == source: continue
                if other_source in source:  # 短 source 挂靠
                    merged_group["merged_sources"].append(other_source)
                    merged_group["candidates"].extend(raw_grouped_inputs[other_source].get("candidates", []))
                    source_aliases[other_source] = source
                    consumed_sources.add(other_source)

        self.grouped_stage_two_inputs = grouped_inputs
        self.grouped_stage_two_source_aliases = source_aliases

        grouped_items = sorted(list(grouped_inputs.values()), key=lambda item: (-len(item["source"]), -len(item["candidates"])))
        batches, current_batch, current_tokens = [], [], 0
        for item in grouped_items:
            item_tokens = Tokener().num_tokens_from_str(json.dumps(item, ensure_ascii=False))
            if current_batch and (current_tokens + item_tokens > self.REDUCE_BATCH_TOKEN_LIMIT):
                batches.append(current_batch)
                current_batch, current_tokens = [], 0
            current_batch.append(item)
            current_tokens += item_tokens
        if current_batch: batches.append(current_batch)

        return batches

    # ------------------------------------------------------------------
    # 成簇模式（POC）：见 EXTRACTION_REFACTOR_PLAN.md
    # ------------------------------------------------------------------

    def _is_cluster_mode(self) -> bool:
        return str(getattr(self.config, "extract_name_cluster_mode", "cluster")).strip() != "legacy_merge"

    def _prepare_reduction_batches_cluster(self, raw_grouped_inputs: dict) -> list:
        """成簇模式：保留每个独立 source，按连通分量成簇后组装发送单元。"""
        clusters = ExtractionClustering.build_clusters(
            list(raw_grouped_inputs.keys()),
            source_language=str(getattr(self.config, "source_language", "") or ""),
            full_text=self._analysis_full_text,
        )

        grouped_inputs, source_aliases = {}, {}
        self.source_frequencies, self.honorific_variant_map, self.collapse_log = {}, {}, []
        self._entry_source_by_id = {}
        unit_payloads, entry_id = [], 100

        for cluster_index, cluster in enumerate(clusters):
            self.source_frequencies.update(cluster.frequencies)

            # 敬称变体：candidates 并入 base，不作为独立 entry
            variant_sources = set()
            for variant, (base, honorific) in cluster.honorific_edges.items():
                self.honorific_variant_map[variant] = (base, honorific)
                source_aliases[variant] = base
                variant_sources.add(variant)

            # 零独立出现折叠：candidates 并入覆盖最多的长词，不作为独立 entry
            collapsed_sources = set()
            for short, targets in cluster.collapse_targets.items():
                source_aliases[short] = targets[0][0]
                collapsed_sources.add(short)
                self.collapse_log.append({
                    "source": short,
                    "targets": [t[0] for t in targets],
                    "reason": "no_independent_occurrence",
                })

            for source in list(cluster.members) + list(variant_sources) + list(collapsed_sources):
                if source in raw_grouped_inputs:
                    grouped_inputs[source] = {
                        "source": source, "merged_sources": [source],
                        "candidates": list(raw_grouped_inputs[source].get("candidates", [])),
                    }
                    source_aliases.setdefault(source, source)

            # 组装发送单元
            for unit_members in ExtractionClustering.split_into_units(cluster):
                entries = []
                for source in unit_members:
                    if source in cluster.honorific_edges:
                        continue  # 敬称变体不作为独立 entry，由回填机制处理
                    if source not in raw_grouped_inputs and source not in cluster.frequencies:
                        continue
                    merged_candidates = list(raw_grouped_inputs.get(source, {}).get("candidates", []))
                    attached_variants = []
                    for variant, (base, _h) in cluster.honorific_edges.items():
                        if base == source:
                            attached_variants.append(variant)
                            merged_candidates.extend(raw_grouped_inputs.get(variant, {}).get("candidates", []))
                    for short, targets in cluster.collapse_targets.items():
                        if targets[0][0] == source:
                            merged_candidates.extend(raw_grouped_inputs.get(short, {}).get("candidates", []))
                    if not merged_candidates:
                        continue

                    total, independent = cluster.frequencies.get(source, (1, 1))
                    entry = {
                        "entry_id": entry_id, "source": source,
                        "count": total, "independent_count": independent,
                        "candidates": ExtractionClustering.aggregate_candidates(merged_candidates),
                    }
                    if attached_variants:
                        entry["honorific_variants"] = attached_variants
                    entries.append(entry)
                    self._entry_source_by_id[entry_id] = source
                    entry_id += 1

                if not entries:
                    continue
                entry_ids = {e["source"]: e["entry_id"] for e in entries}
                relations = [
                    {"type": "contains", "short": entry_ids[s], "long": entry_ids[l], "shared_fragment": frag}
                    for s, l, frag in cluster.contain_edges
                    if s in entry_ids and l in entry_ids
                ]
                unit_payloads.append({"cluster_id": cluster_index, "entries": entries, "relations": relations})

        self.grouped_stage_two_inputs = grouped_inputs
        self.grouped_stage_two_source_aliases = source_aliases

        if self.collapse_log:
            self.info(f"零独立出现折叠 {len(self.collapse_log)} 个短词: " + "、".join(
                f"{c['source']}→{c['targets'][0]}" for c in self.collapse_log[:10]))
        if self.honorific_variant_map:
            self.info(f"检测到 {len(self.honorific_variant_map)} 个敬称变体，将在最终阶段本地回填译名。")

        # token 分批，发送单元为原子
        batches, current_batch, current_tokens = [], [], 0
        for unit in unit_payloads:
            unit_tokens = Tokener().num_tokens_from_str(json.dumps(unit, ensure_ascii=False))
            if current_batch and (current_tokens + unit_tokens > self.REDUCE_BATCH_TOKEN_LIMIT):
                batches.append(current_batch)
                current_batch, current_tokens = [], 0
            current_batch.append(unit)
            current_tokens += unit_tokens
        if current_batch: batches.append(current_batch)
        return batches

    def _build_second_stage_prompt(self, batch: list) -> tuple[str, list[dict]]:
        """第二阶段：候选归并与 AI 裁决（优化版提示词）"""
        system_prompt = (
            "你是一个本地化术语库规范化专家。你将收到一组经初步提取的“候选词组（Group）”。\n"
            "有时候相同的词汇会被误判为不同的类型（如既被识别为角色，又被识别为术语）。\n"
            "你的唯一任务是：综合判定每个 Group，裁决它最终属于“角色(characters)”还是“术语(terms)”，并提炼出一个最准确的结果。\n"
            "【严格执行以下规则】\n"
            "1. 唯一归属：同一个词不能既是角色又是术语，必须二选一。\n"
            "2. 主键保留：输出的 `source` 必须严格使用传入的 `主source`，绝不能随意篡改。\n"
            "3. 丢弃无价值词汇：如果某个 Group 里的词汇看起来是普通词语（如“今天”、“然后”），请直接忽略，不要输出它。\n"
            "4. 信息整合：如果推荐译名或备注有多个参考，请合并为你认为最合理的版本。\n"
            "【输出格式】\n"
            "必须输出合法的 JSON 代码块，严格遵守以下结构：\n"
            "```json\n"
            "{\n"
            "  \"characters\": [{\"source\": \"主source\", \"recommended_translation\": \"推荐译名\", \"gender\": \"分类属性\", \"note\": \"整合后的备注\"}],\n"
            "  \"terms\": [{\"source\": \"主source\", \"recommended_translation\": \"推荐译名\", \"category_path\": \"分类属性\", \"note\": \"整合后的备注\"}]\n"
            "}\n"
            "```"
        )
        
        # 优化 Few-Shot：展示如何解决类型冲突和合并备注
        sample_group = [
            {
                "source": "亚瑟王",
                "merged_sources": ["亚瑟王", "亚瑟"],
                "candidates": [
                    {"type": "character", "recommended_translation": "King Arthur", "gender": "男性", "note": "历史人物"},
                    {"type": "term", "recommended_translation": "Arthur", "category_path": "称号", "note": "错误分类为术语"}
                ]
            },
            {
                "source": "月光斩",
                "merged_sources": ["月光斩"],
                "candidates": [
                    {"type": "term", "recommended_translation": "Moon Slash", "category_path": "技能", "note": "剑技名称"}
                ]
            },
            {
                "source": "精灵族",
                "merged_sources": ["精灵族"],
                "candidates": [
                    {"type": "term", "recommended_translation": "Elves", "category_path": "种族", "note": "种族名称"}
                ]
            }
        ]
        import rapidjson as json
        
        fake_user = (
            "请分析以下候选组并完成合并裁决，`recommended_translation` 必须写成简体中文。\n"
            f"---\n{json.dumps(sample_group, ensure_ascii=False)}\n---\n"
            "请输出 JSON 合并结果。"
        )
        fake_assistant = (
            "```json\n"
            "{\n"
            "  \"characters\": [\n"
            "    {\"source\": \"亚瑟王\", \"recommended_translation\": \"King Arthur\", \"gender\": \"男性\", \"note\": \"历史人物\"}\n"
            "  ],\n"
            "  \"terms\": [\n"
            "    {\"source\": \"月光斩\", \"recommended_translation\": \"Moon Slash\", \"category_path\": \"技能\", \"note\": \"剑技名称\"},\n"
            "    {\"source\": \"精灵族\", \"recommended_translation\": \"Elves\", \"category_path\": \"种族\", \"note\": \"种族名称\"}\n"
            "  ]\n"
            "}\n"
            "```"
        )
        language_requirement = self._get_recommended_translation_language_requirement()
        user_prompt = (
            f"请分析以下候选组并完成合并裁决，{language_requirement}\n"
            f"---\n{json.dumps(batch, ensure_ascii=False)}\n---\n"
            "请输出 JSON 合并结果。"
        )
        
        messages = [
            {"role": "user", "content": fake_user},
            {"role": "assistant", "content": fake_assistant},
            {"role": "user", "content": user_prompt},
        ]
        return system_prompt, messages


    # ========================================================================
    # 4. 最终阶段：整合兜底与落盘 (分)
    # ========================================================================

    def _get_candidate_occurrence_count(self, source: str, candidate_type: str) -> int:
        source = str(source or "").strip()
        candidate_type = str(candidate_type or "").strip()
        # 成簇模式：优先使用本地统计的原文真实出现次数
        if source in self.source_frequencies:
            total, _independent = self.source_frequencies[source]
            if total > 0:
                return total
        grouped_item = self.grouped_stage_two_inputs.get(source) or {}
        occurrence_count = sum(
            1
            for candidate in grouped_item.get("candidates", []) or []
            if candidate.get("type") == candidate_type
            and str(candidate.get("candidate_source", "")).strip() == source
        )
        return max(1, occurrence_count)

    def _finalize_results(self, first_stage_results: list, second_stage_results: list) -> dict:
        """主线程收口：采纳 AI 裁决结果 -> 启发式兜底缺失项 -> 清洗禁翻项"""
        merged_characters, merged_terms, assigned_sources = {}, {}, set()

        # 0. (成簇模式) 收集 AI 有意丢弃的词，兜底时跳过
        discarded_sources = {
            str(item.get("source", "")).strip()
            for result in second_stage_results
            for item in result.get("discarded", []) or []
            if isinstance(item, dict)
        }

        # 1. 吸收并规范化第二阶段 AI 的结果
        for result in second_stage_results:
            for row in result.get("characters", []):
                source = str(row.get("source", "")).strip()
                source = self.grouped_stage_two_source_aliases.get(source, source)
                if not source or source in assigned_sources: continue
                merged_characters[source] = {
                    "source": source, "recommended_translation": str(row.get("recommended_translation", "")).strip(),
                    "gender": str(row.get("gender", "")).strip() or "其他", "note": str(row.get("note", "")).strip(),
                    "occurrence_count": self._get_candidate_occurrence_count(source, "character"),
                }
                assigned_sources.add(source)

            for row in result.get("terms", []):
                source = str(row.get("source", "")).strip()
                source = self.grouped_stage_two_source_aliases.get(source, source)
                if not source or source in assigned_sources: continue
                merged_terms[source] = {
                    "source": source, "recommended_translation": str(row.get("recommended_translation", "")).strip(),
                    "category_path": str(row.get("category_path", "")).strip() or "其他", "note": str(row.get("note", "")).strip(),
                    "occurrence_count": self._get_candidate_occurrence_count(source, "term"),
                }
                assigned_sources.add(source)

        # 2. 对 AI 未处理的组进行启发式程序兜底
        for source, grouped_item in self.grouped_stage_two_inputs.items():
            if source in assigned_sources: continue
            if source in discarded_sources: continue  # AI 有意丢弃的普通词不回捡
            # 成簇模式：敬称变体与被折叠词不独立兜底（由回填/折叠机制处理）
            if source in self.honorific_variant_map: continue
            if self.grouped_stage_two_source_aliases.get(source, source) != source: continue

            c_cands = [c for c in grouped_item.get("candidates", []) if c.get("type") == "character"]
            t_cands = [c for c in grouped_item.get("candidates", []) if c.get("type") == "term"]
            
            prefer_term = False
            if t_cands and not c_cands: prefer_term = True
            elif t_cands and c_cands:
                t_score = len(t_cands) + sum(1 for c in t_cands if str(c.get("category_path", "")).strip() not in ["", "其他"])
                c_score = len(c_cands) + sum(1 for c in c_cands if str(c.get("gender", "")).strip() not in ["", "其他"])
                prefer_term = t_score > c_score

            notes = []
            all_cands = t_cands + c_cands if prefer_term else c_cands + t_cands
            trans = next((str(c.get("recommended_translation", "")).strip() for c in all_cands if str(c.get("recommended_translation", "")).strip()), "")
            
            for c in all_cands:
                if n := str(c.get("note", "")).strip():
                    if n not in notes: notes.append(n)

            if prefer_term:
                cat = next((str(c.get("category_path", "")).strip() for c in all_cands if str(c.get("category_path", "")).strip() not in ["", "其他"]), "其他")
                merged_terms[source] = {
                    "source": source, "recommended_translation": trans, "category_path": cat, "note": " | ".join(notes),
                    "occurrence_count": self._get_candidate_occurrence_count(source, "term"),
                }
            else:
                gen = next((str(c.get("gender", "")).strip() for c in all_cands if str(c.get("gender", "")).strip() not in ["", "其他"]), "其他")
                merged_characters[source] = {
                    "source": source, "recommended_translation": trans, "gender": gen, "note": " | ".join(notes),
                    "occurrence_count": self._get_candidate_occurrence_count(source, "character"),
                }

        # 3. 第一阶段禁翻项的清洗合并 (不走第二阶段 AI)
        merged_non_translate = {}
        for result in first_stage_results:
            for row in result.get("non_translate", []):
                marker = str(row.get("marker", "")).strip()
                if not marker or all(char.isspace() or char in self.COMMON_PUNCTUATION_CHARS for char in marker):
                    continue

                cat, note = str(row.get("category", "")).strip(), str(row.get("note", "")).strip()
                existing = merged_non_translate.setdefault(
                    marker,
                    {"marker": marker, "category": cat, "note": note, "occurrence_count": 0},
                )
                existing["occurrence_count"] = int(existing.get("occurrence_count", 0) or 0) + 1
                
                if (not existing["category"] or existing["category"] == "其他") and cat and cat != "其他": existing["category"] = cat
                if not existing["note"] and note: existing["note"] = note

        # 4. (成簇模式) 敬称变体本地回填：名字部分随 base 统一，敬称后缀沿用第一阶段译法
        self._backfill_honorific_variants(merged_characters, merged_terms)

        return {
            "characters": list(merged_characters.values()),
            "terms": list(merged_terms.values()),
            "non_translate": list(merged_non_translate.values()),
        }

    def _backfill_honorific_variants(self, merged_characters: dict, merged_terms: dict) -> None:
        for variant, (base, _honorific) in self.honorific_variant_map.items():
            if variant in merged_characters or variant in merged_terms:
                continue
            base_row = merged_characters.get(base) or merged_terms.get(base)
            if not base_row:
                continue
            grouped_item = self.grouped_stage_two_inputs.get(variant) or {}
            variant_old = next(
                (str(c.get("recommended_translation", "")).strip()
                 for c in grouped_item.get("candidates", []) or []
                 if str(c.get("recommended_translation", "")).strip()),
                "",
            )
            base_old_candidates = [
                str(c.get("recommended_translation", "")).strip()
                for c in (self.grouped_stage_two_inputs.get(base) or {}).get("candidates", []) or []
            ]
            base_new = str(base_row.get("recommended_translation", "")).strip()
            derived = self._derive_variant_translation(base_old_candidates, base_new, variant_old)
            if not derived:
                continue

            target_table = merged_characters if base in merged_characters else merged_terms
            row = {
                "source": variant,
                "recommended_translation": derived,
                "note": f"敬称变体，基于 {base}",
                "occurrence_count": self._get_candidate_occurrence_count(variant, "character"),
            }
            if target_table is merged_characters:
                row["gender"] = str(base_row.get("gender", "")).strip() or "其他"
            else:
                row["category_path"] = str(base_row.get("category_path", "")).strip() or "其他"
            target_table[variant] = row
        if self.honorific_variant_map:
            backfilled = sum(
                1 for v in self.honorific_variant_map
                if v in merged_characters or v in merged_terms
            )
            self.info(f"敬称变体回填完成：{backfilled} / {len(self.honorific_variant_map)} 个。")

    @staticmethod
    def _derive_variant_translation(base_old_candidates: list, base_new: str, variant_old: str) -> str:
        """
        变体译名推导：variant_old 若以 base 的某个旧译名开头，则把前缀替换为 base_new，
        保留敬称后缀的第一阶段译法（如 阿瑟先生 + 阿瑟→亚瑟 = 亚瑟先生）。
        推导失败时返回 variant_old（保留原译名，只是名字部分可能不统一）。
        """
        if not base_new:
            return variant_old
        if not variant_old:
            return base_new
        for base_old in sorted(set(filter(None, base_old_candidates)), key=len, reverse=True):
            if variant_old.startswith(base_old) and variant_old != base_old:
                return base_new + variant_old[len(base_old):]
            if variant_old == base_old:
                return base_new
        return variant_old


    # ========================================================================
    # 调试辅助：第一阶段快照与重放（见 EXTRACTION_REFACTOR_PLAN.md 第 8 节）
    # ========================================================================

    def _stage1_dump_path(self) -> str:
        output_path = str(getattr(self.config, "label_output_path", "") or "").strip() or "."
        return os.path.join(output_path, "debug_stage1_dump.json")

    def _maybe_dump_stage1(self, first_stage_results: list) -> None:
        """配置 extract_debug_dump_switch=true（手写配置文件，不进 UI）时保存第一阶段快照。"""
        if not getattr(self.config, "extract_debug_dump_switch", False):
            return
        try:
            dump_path = self._stage1_dump_path()
            with open(dump_path, "w", encoding="utf-8") as writer:
                json.dump({
                    "created_at": datetime.now().isoformat(),
                    "config_digest": {
                        "model": str(getattr(self.config, "model", "")),
                        "source_language": str(getattr(self.config, "source_language", "")),
                        "target_language": str(getattr(self.config, "target_language", "")),
                    },
                    "full_text": self._analysis_full_text,
                    "first_stage_results": first_stage_results,
                }, writer, ensure_ascii=False, indent=2)
            self.info(f"[调试] 第一阶段快照已保存: {dump_path}")
        except Exception as error:
            self.warning(f"[调试] 第一阶段快照保存失败: {error}")

    def _try_load_stage1_replay(self) -> list | None:
        """配置 extract_debug_replay_path 指向快照文件时，跳过第一阶段直接重放。"""
        replay_path = str(getattr(self.config, "extract_debug_replay_path", "") or "").strip()
        if not replay_path:
            return None
        try:
            with open(replay_path, "r", encoding="utf-8") as reader:
                dump = json.load(reader)
            full_text = str(dump.get("full_text", "") or "")
            if full_text:
                self._analysis_full_text = full_text
            digest = dump.get("config_digest", {}) or {}
            if str(digest.get("source_language", "")) != str(getattr(self.config, "source_language", "")):
                self.warning("[调试] 快照的 source_language 与当前配置不一致，频率统计与敬称剥离可能不准确。")
            return list(dump.get("first_stage_results", []) or [])
        except Exception as error:
            self.warning(f"[调试] 快照重放加载失败，回退到正常第一阶段: {error}")
            return None


    # ========================================================================
    # 通用工具
    # ========================================================================

    def _calculate_progress_percent(self, phase: str, current: int = 0, total: int = 0) -> int:
        """根据阶段和进度计算总体百分比"""
        current, total = max(0, int(current or 0)), max(0, int(total or 0))
        if phase == "prepare": return 0
        if phase == "stage1": return 10 + int((current / total) * 50) if total > 0 else 60
        if phase == "stage2": return 60 + int((current / total) * 30) if total > 0 else 90
        if phase == "finalize": return 90 + int((current / total) * 10) if total > 0 else 90
        return 0

    def _emit_progress_update(self, phase: str, phase_label: str, current: int, total: int, message: str, detail: str) -> None:
        """统一的进度更新接口"""
        self.emit(
            Base.EVENT.ANALYSIS_TASK_UPDATE,
            {
                "status": "running", "phase": phase, "phase_label": phase_label,
                "current": max(0, int(current or 0)), "total": max(0, int(total or 0)),
                "percent": self._calculate_progress_percent(phase, current, total),
                "message": message, "detail": detail,
            },
        )

    # ---------------------------------------------------------
    # ★ 完整的请求发送与回复检查器 (一、二阶段共用流水线)
    # ---------------------------------------------------------
    
    def _execute_analysis_request(
        self, 
        stage_label: str, 
        system_prompt: str, 
        messages: list[dict], 
        required_fields: tuple[str, ...], 
        custom_validator=None
    ) -> dict | None:
        """
        大模型请求的统一流水线：
        请求发送 -> 异常捕获 -> JSON 提取 -> 基础字段校验 -> 自定义业务校验 -> 数据清洗 -> 失败重试
        """
        last_error = "未知错误"

        for attempt in range(1, self.MAX_REQUEST_ATTEMPTS + 1):
            if Base.work_status == Base.STATUS.STOPING:
                return None

            try:
                # 1. 发送请求
                requester = LLMRequester()
                skip, _, response_content, _, _ = requester.sent_request(
                    [dict(msg) for msg in messages],
                    system_prompt,
                    self.config.get_active_platform_configuration(),
                )

                if skip:
                    if Base.work_status == Base.STATUS.STOPING: return None
                    last_error = "请求被跳过或接口返回错误"
                    continue

                response_content = str(response_content or "").strip()
                if not response_content:
                    last_error = "模型回复为空"
                    continue

                # 2. 从回复中提取 JSON (兼容多余文字)
                parsed_json = self._extract_json_from_text(response_content)
                if not parsed_json:
                    last_error = "未匹配到合法的 JSON 结构"
                    continue

                # 3. 基础结构校验 (确保必须字段存在且为列表)
                is_valid, validation_error = self._validate_base_structure(parsed_json, required_fields)
                if not is_valid:
                    last_error = validation_error
                    continue

                # 4. 阶段自定义业务校验 (如需要)
                if custom_validator:
                    is_valid, validation_error = custom_validator(parsed_json)
                    if not is_valid:
                        last_error = validation_error
                        continue

                # 5. 成功：归一化并清洗无用字段，安全返回
                return {field: list(parsed_json.get(field) or []) for field in required_fields}

            except Exception as error:
                if Base.work_status == Base.STATUS.STOPING: return None
                last_error = str(error)

            # 记录重试日志
            if attempt < self.MAX_REQUEST_ATTEMPTS:
                self.warning(f"{stage_label} 第 {attempt} 次尝试失败，将重试一次：{last_error}")
            else:
                self.error(f"{stage_label} 第 {attempt} 次尝试失败：{last_error}")

        return None

    def _extract_json_from_text(self, text: str) -> dict | None:
        """辅助方法：正则提取被包裹的 JSON"""
        json_match = re.search(r"\{.*\}", text, re.DOTALL)
        if json_match:
            try:
                parsed = json.loads(json_match.group(0))
                if isinstance(parsed, dict): return parsed
            except json.JSONDecodeError:
                pass
        return None

    def _validate_base_structure(self, parsed: dict, required_fields: tuple[str, ...]) -> tuple[bool, str]:
        """辅助方法：校验基础结构"""
        for field in required_fields:
            if field not in parsed: return False, f"缺少必须字段: {field}"
            if not isinstance(parsed.get(field), list): return False, f"字段 {field} 必须是数组"
        return True, ""
