#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
BSCAT 分类模型效果监控

启动时先执行 src/sync.py 到线上取数：5 张 dash 表写入 input/；
随后聚合出监控指标，渲染成单文件 HTML 看板，文件名为
output/bscat_monitor_<运行时间戳>.html。

用法:
    python monitor.py
    python monitor.py --skip-sync   # 跳过线上取数，直接用 input/ 现有文件出图
"""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# 同步、模板、规则配置都在 src/ 下，和 monitor.py 分开放
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from sync import sync_all

# Windows 控制台默认 GBK 编码，日志里出现一个 ⚠ 或 ✗，print 就抛 UnicodeEncodeError。
# 糟的是这类字符恰好都在「出事了要喊一声」的路径上——提醒自己把自己干掉，
# 而且报出来的是一串调用栈，不是那条该看的警告。
# 保留原编码（中文在 GBK 控制台上显示是正常的），只把编不出来的字符降级成 ?。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(errors="replace")
    except (AttributeError, ValueError):
        pass  # 不是 TextIOWrapper（被重定向 / 被包过）就算了，不影响正常路径

# ── 常量 ──────────────────────────────────────────────────────────────────
# flow 表不在清单里：第三部分的流向明细一律从 detail 表现算，这样点开类别看到的
# 各流向之和恒等于类别层的差异笔数。多留一条并行的聚合路径，迟早会算出两个数。
FILES = {
    "summary":  "risk_000008_dash_summary.csv",
    "trend":    "risk_000008_dash_trend.csv",
    "group":    "risk_000008_dash_group.csv",
    "category": "risk_000008_dash_category.csv",
    "detail":   "risk_000008_dash_detail.csv",
}

# summary 表 ord -> 载荷字段名
SUMMARY_KEYS = {
    1:  "total",
    2:  "illion_coverage",
    3:  "finv_coverage",
    4:  "agreement_rate",
    5:  "mismatch_rate",
    6:  "coverage_adjusted_agreement",
    7:  "all_difference_rate",
    8:  "coverage_overlap_rate",
    9:  "coverage_gap_rate",
    10: "dir_diff_vs_illion",
    11: "dir_diff_vs_finv",
    12: "all_difference_count",
    13: "illion_only",
    14: "finv_only",
    15: "both_empty",
}

# ── 双语 ──────────────────────────────────────────────────────────────────
# 载荷里**给人看的每一句**都成对发过去（{"zh": …, "en": …}），页面按当前语言取一半
# （template.html 的 tx()）。**当键使的中文不成对**：类别名、group、EMPTY_LABEL、
# GLOBAL_CATEGORY、枚举值、源表列名、差异类型的 alias——它们是查表与自检的锚点，
# 翻译了就和历史库、和源表对不上号。哪些是文案、哪些是键，这一条线不能含糊。
#
# 为什么是成对而不是「跑两遍、按语言取一份」：载荷只有一份，页面开在哪个语言是**打开
# 那一刻**才知道的（跟随浏览器 / 本地记忆 / #lang=），生成时不知道。成对发过去，
# 页面不必重跑一遍脚本就能换语言，代价只是页面里每句话多存一份译文。
def bi(zh: str, en: str) -> dict:
    """一条文案的两种写法。"""
    return {"zh": zh, "en": en}


def unipair(s: str) -> dict:
    """两种语言写法完全相同的一条（纯数字、单位符号、金额）。成对的形状保持一致，
    取用方不必知道哪一条其实不必翻。"""
    return {"zh": s, "en": s}


def joina(parts: list[dict], sep: dict) -> dict:
    """把成对的片段按**成对的分隔符**拼成一对。

    分隔符也得成对：中文那句用全角空格把「本期：」隔开，英文写成「 — this run:」
    是另一套写法，不是同一个符号换了个语言。
    """
    return {"zh": sep["zh"].join(p["zh"] for p in parts),
            "en": sep["en"].join(p["en"] for p in parts)}


# 差异类型：(key, 展示标签, 配色变量, 说明, detail 表别名)
# 排列顺序 red -> violet -> yellow 是配色校验的一部分，紫色居中才能通过色盲门槛，不要调整。
# 展示标签里的 bscat 指 finv 侧模型输出，和源表「finv」是同一样东西，只是换个更好懂的名字；
# 别名（第 5 项）必须原样保留源表用词，改了就取不到数——所以它在双语改造里不动。
MISMATCH_TYPES = [
    ("miss",  bi("bscat 漏识别", "BSCAT missed"), "var(--m-miss)",
     bi("illion 有分类、bscat 为空。模型完全没识别出来，优先排查。",
        "Illion has a category, BSCAT is empty — the model missed it outright. Look here first."),
     "finv漏识别"),
    ("diff",  bi("分类不一致", "Category mismatch"), "var(--m-diff)",
     bi("两侧都有分类但结果不同，属于分类边界冲突或规则优先级问题。",
        "Both sides have a category but they disagree — a boundary conflict or a rule-priority issue."),
     "分类边界冲突"),
    ("extra", bi("bscat 多识别", "BSCAT extra"), "var(--m-extra)",
     bi("bscat 有分类、illion 为空。可能是模型过度识别，也可能是 illion 漏标。",
        "BSCAT has a category, Illion is empty — either the model over-classified, or Illion missed the label."),
     "finv新增识别"),
]

# detail 表的「排查类型」与 flow 表的「差异类型」用词不同，都要能映射回同一组 key
TYPE_BY_LABEL = {t[4]: t[0] for t in MISMATCH_TYPES}
TYPE_BY_LABEL.update({
    "仅illion有分类": "miss",
    "分类不一致": "diff",
    "仅finv有分类": "extra",
})

# ── 受控枚举（Enum）───────────────────────────────────────────────────────
# 看板上所有「可分类字段」（问题类型 / 任务状态 / 优先级 / 推荐动作 / 差异类型 /
# 变化方向）只允许取下面这些值。名单只在这一处定义，随载荷发给页面
# （payload["enums"]），JS 侧写的是 const ENUMS = DATA.enums——不另抄一份，
# 抄两份迟早分叉，而分叉的症状是「同一个意思两个词并存」，正是枚举要防的事。
#
# 页面拿到任何不在这份名单里的值，一律走 ENUM_FALLBACK 落到受控值（见 JS 的
# normalizeEnum）；原始数据里的自由文本不改，只在进看板时做映射。
ENUMS = {
    # 问题类型：这一条差异「是什么问题」，不是「怎么处理」（怎么处理见 recommendedAction）
    "issueType": [
        "Category Definition", "Missing Rule", "Merchant Mapping",
        "Institution Mapping", "Rule Priority", "Insufficient Information",
        "Model Logic", "Illion Difference", "Other",
    ],
    # 任务状态：To-Do 卡片的处理进度。人推进，由 New 起、Closed 止；
    # 指标恢复**不会**把它自动关掉——关不关是人的判断，脚本不替人做。
    "taskStatus": [
        "New", "Investigating", "Need QA", "KB/Rule Updated", "Validating", "Closed",
    ],
    # 推荐动作：这一条待办「打算怎么处理」。由规则表（MONITORING_RULES）直接给出，
    # 不按数据猜——同一个问题在页面上永远配同一个动作，长期统计才做得起来。
    # 与 issueType 是一件事的两面（是什么问题 / 怎么处理），不能一个字段兼两用。
    "recommendedAction": [
        "Review Category Definition", "Review Missing Rule", "Review Merchant Mapping",
        "Review Institution Mapping", "Adjust Rule Priority",
        "Review Insufficient Information", "Review Model Logic",
        "Review Illion Difference", "No Action",
    ],
    # 差异类型：沿用现有三类差异的业务含义（漏识别 / 不一致 / 多识别），
    # 中文标签仍作展示用，受控值用这三个（映射见 DIFF_TYPE_ENUM）。
    # Other 不是第四类差异，是落点：数据里出现没映射过的类型时用它兜底，
    # 正常数据下恒不出现（那种情况下自检本身就会报「有 N 笔未标注差异类型」）。
    "differenceType": ["Miss", "Category Difference", "Extra", "Other"],
    # 变化方向：本期相对上一监控日的走向。只有三档，且**不含程度词**——
    # 「略微下降」「显著下降」这类词把「变了多少」和「朝哪边变」揉进一个字段；
    # 这里只答后一个问题，变了多少看 delta，要不要紧由读的人对着规则判。
    "changeDirection": ["Increase", "Decrease", "Stable"],
}

# 非法值的落点。按字段的业务含义分别定，不一律变成 Other：
# 状态落回 New（未开工）、方向落 Stable（取不到方向就是没看出方向）。
ENUM_FALLBACK = {
    "issueType": "Other",
    "taskStatus": "New",
    "recommendedAction": "No Action",
    "differenceType": "Other",
    "changeDirection": "Stable",
}

# detail 表的差异类型 key -> 受控值。看板展示受控值 + 中文标签（见 mismatchTypes 的 enum 字段）。
DIFF_TYPE_ENUM = {"miss": "Miss", "diff": "Category Difference", "extra": "Extra"}

EMPTY_TOKENS = {"(空)", "", "(空值)", "nan", "None"}
EMPTY_LABEL = "（未分类）"

# 明细里出现没映射过的排查类型时的落点：不并进任何一个已知类型（并进去等于悄悄改了
# 构成比），单独计数由自检喊出来。正常数据下这个数恒为 0。
UNLABELED_TYPE = "（未标注）"

# ── 每日监控：没有批次、也没有 Run ────────────────────────────────────────
# 实际用法是「每天跑一次脚本，抽当天一部分样本，基于当天样本算指标，和历史比」。
# 因此不存在 sample_batch_id / run_id / model_version 这三个概念：既没有数据源，
# 也没法从交易明细里倒推出来（编一个出来，就会把「样本换了」读成「模型改了」）。
# 历史库里那三列随之退役——旧值不再有用，由 ALLOWED_DROPS 显式放行丢弃。
# 页面上也不再出现 Sample Batch / Batch ID / Model Batch 任何一个词。

# 趋势源表里已有的列
TREND_SRC_COLS = [
    "par_dt", "total_rows", "illion_coverage", "finv_coverage",
    "agreement_rate", "mismatch_count", "mismatch_rate",
    "coverage_adjusted_agreement", "all_difference_count",
    "all_difference_rate", "illion_only_count", "finv_only_count",
]

# 源表没有、每次运行现算的列——决策信号全在这里。
# 前五个率以「并集」为分母，这样它们之间可以直接横向比、也能互相加减；
# unknown_rate 是唯一的例外，分母是**全量样本**：双方都没分类的交易既不在并集里、
# 也不在任何一侧的覆盖里，只有总样本数是它的分母（口径见页脚）。
TREND_EXTRA_COLS = [
    "union_rows",              # 并集笔数 = 至少一侧给出分类
    "both_class_rows",         # 双方都有分类
    "both_empty_rows",         # 双方都无分类
    "coverage_overlap_rate",   # 双方都有分类 ÷ 并集：上游数据漂移的信号
    "miss_rate",               # 漏识别 ÷ 并集：模型能力退化的信号
    "extra_rate",              # 多识别 ÷ 并集
    "unknown_rate",            # 双方都无分类 ÷ 全量样本：连两侧都判不了的量
    # 三个分子：覆盖率与一致率的原始笔数。rates 已经写在上面的列里，但那是比值——
    # 页面上的卡片写的是「244,085 ÷ 269,790」这种原始笔数，比值乘以分母再取整
    # 会在最后一位上飘，而卡片和图表用的是同一份数，飘了就是对不上。
    # 历史行没有这三列时留空，页面显示「—」：不拿比值反推，反推出来的数没人复核过。
    "illion_coverage_num",
    "finv_coverage_num",
    "agreement_num",           # 两侧标签相同的笔数（一致率与全局一致率共用的分子）
    # 差异金额：差异交易的 |amount| 合计，按「illion 侧优先」归因（与分类别的
    # diff_amount 同源，见 build_categories）。以前只进页头规模卡而不入趋势，
    # 于是「差异金额涨了多少」在页面上无从看起。首期写 0 是有值的，
    # 但更早的历史行没有这一列——那些行留空，图上断开，不拿笔数去反推金额。
    "mismatch_amount",
]

TREND_COLS = TREND_SRC_COLS + TREND_EXTRA_COLS

# 允许在 upsert_trend 里被静默丢掉的列。reindex(columns=TREND_COLS) 不会为「丢掉
# 一列」报任何错——将来 TREND_EXTRA_COLS 里写错一个名字，就是每天安静地少一列真实
# 数据，页面上那一天的值变成空白，没有任何别的症状。所以退役列必须点名放行。
ALLOWED_DROPS = {"sample_batch_id", "run_id", "model_version"}

# 类别优先级（2026-10-13 用户定）：**只针对类别**，与规则无关（规则不再带 P）。
#   P0 = 收入类 + 贷款类两个板块的全体成员
#   P1 = 下面点名的这几个
#   P2 = 其余
# 板块名来自 input/risk_000008_dash_group.csv 的「业务大类」列（见 build_segments）。
# 板块名或类别名拼错一个不会有别的症状——只在这一档少一枚徽标、排序档里也少几行，
# 所以自检盯着各档的成员数（见 main 里那段「类别 P 档必须是一份划分」）。
LEVEL_BY_SEGMENT = {"收入类": "P0", "贷款类": "P0"}
LEVEL_BY_NAME = {"Rent": "P1", "Gambling": "P1"}
LEVELS = ("P0", "P1", "P2")

# ① 顶部指标卡的键。template.html 的 TILES 是视觉来源，这里是**校验**用的名单：
# 全局按日规则命中后只能挂在这些卡上（配置里的 tile），写错一个字母角标就静默不出现，
# 所以在加载配置时当场拦。两处名单要一起改——模板加了一张卡，这里也要加。
TILE_METRIC_KEYS = ("illion_coverage", "bscat_coverage", "agreement",
                    "global_agreement", "miss", "extra")

# ── 监控规则（Monitoring Rules）───────────────────────────────────────────
# 规则决定「要不要生成运维待办、生成哪一类」。没命中的类别不会出现在 To-Do 列表里，
# 页面上也没有第二条路能造出一条待办——「规则决定有没有」和「枚举限制值域」是两件事，
# 分开管：前者是配置里的规则表，后者是 ENUMS。
#
# 规则本身（阈值、开关、连续天数、基准窗口）都在 rules_config.json 里，
# 这一段只留「怎么判」——指标的取法（ALERT_METRICS）与比较逻辑。改阈值改配置、
# 改判据改代码，两者不再混在同一个常量里。
#
# 两段规则的分工：
#   snapshot —— 只看监控日当天的数（window=1）。RULE_001/002/003 原来在这里，
#               2026-10-16 用户定「规则只保留趋势的规则，不保留当天的」，
#               三条连同它们的待办一起删了，这一段落空（引擎与加载路径都还在，
#               以后要加当期规则，写回配置里那一段即可）。
#   alerts   —— 带连续天数或历史基准，要看多期历史才判得出来。判出来的是「预警」，
#               运维事项由预警生成。2026-11-13 用户把预警规则也全部移除了，这一段同样为空。
# 两段共用同一套判据引擎，也共用同一个待办身份（类别 + 规则 ID）。

# 告警判据能用的指标。名单在这里而不在配置文件里：配置文件给的是「跟谁比、比多少」，
# 指标怎么从类别行里取出来是代码的事（口径只能有一处）。配置文件里写了一个不在这份
# 名单里的指标名，加载时当场报错，不让它变成一条永远不触发的规则。
#
#   src     从「每类每天一行」的序列里取哪个键
#   unit    值的单位，决定页面上怎么印：rate = 0-1 比率印成 %，pt = 百分点，
#           count = 笔数，money = 金额
#   pt      源列是比率，判据用百分点（× 100）。覆盖率差就是这么定的：
#           配置里写 1.0 表示 1 pt，与页脚规则表上那句「> 1 pt」是同一个数
#   abs     判据用绝对值（覆盖率差正负两个方向都要报）
#   short   证据行上的短名。label 是给规则表和悬停用的长名，摆进一句话里太占地方
#   global  全局域规则从这个 metrics 键取值（不分类别的指标，只此一条路径）
ALERT_METRICS = {
    "iou_rate":        {"label": bi("一致率（交集÷并集）", "Agreement (intersection ÷ union)"),
                        "short": bi("一致率", "Agreement"),
                        "unit": "rate",  "src": "iou_rate"},
    "finv_coverage":   {"label": bi("bscat 覆盖率", "BSCAT coverage"),
                        "short": bi("bscat 覆盖率", "BSCAT coverage"),
                        "unit": "rate",  "src": "finv_coverage"},
    "illion_coverage": {"label": bi("illion 覆盖率", "Illion coverage"),
                        "short": bi("illion 覆盖率", "Illion coverage"),
                        "unit": "rate",  "src": "illion_coverage"},
    "coverage_gap":    {"label": bi("覆盖率差（bscat − illion）", "Coverage gap (BSCAT − Illion)"),
                        "short": bi("覆盖率差", "Coverage gap"),
                        "unit": "pt", "src": "coverage_gap", "pt": True},
    "coverage_gap_pt": {"label": bi("覆盖率差（绝对值）", "Coverage gap (absolute)"),
                        "short": bi("覆盖率差", "Coverage gap"),
                        "unit": "pt", "src": "coverage_gap", "pt": True, "abs": True},
    "diff_rate":       {"label": bi("差异率（本类差异 ÷ 本类并集）", "Difference rate (category differences ÷ category union)"),
                        "short": bi("差异率", "Difference rate"),
                        "unit": "rate", "src": "diff_rate"},
    "diff_count":      {"label": bi("差异笔数", "Differences"),
                        "short": bi("差异笔数", "Differences"),
                        "unit": "count", "src": "diff_count"},
    "diff_amount":     {"label": bi("差异金额", "Difference amount"),
                        "short": bi("差异金额", "Difference amount"),
                        "unit": "money", "src": "diff_amount"},
    "unknown_rate":    {"label": bi("未分类率（双方均无分类 ÷ 全量样本）", "Unclassified rate (neither side classified ÷ all rows)"),
                        "short": bi("未分类率", "Unclassified rate"),
                        "unit": "rate", "global": "unknown",
                        "num_label": bi("双方均无分类", "Neither side classified"),
                        "den_label": bi("总样本", "All rows")},
    # 整体域指标：src 是历史趋势里的列（判据要回看历史），global 是 build_metrics
    # 里的当期卡（证据行要拿分子分母）。两个都给的指标只给全局规则用；global_only
    # 让类别规则不能误用——类别行里也有 finv_coverage 这些列，不挡的话一条 category
    # 规则写错指标名会静默地按「本类覆盖率」判。
    "overall_bscat_coverage": {
        "label": bi("整体 bscat 覆盖率（全量样本）", "Overall BSCAT coverage (all rows)"),
        "short": bi("整体 bscat 覆盖率", "Overall BSCAT coverage"),
        "unit": "rate", "src": "finv_coverage", "global": "bscat_coverage",
        "num_label": bi("bscat 已分类", "BSCAT classified"),
        "den_label": bi("全量样本", "All rows"),
        "global_only": True},
    "overall_illion_coverage": {
        "label": bi("整体 illion 覆盖率（全量样本）", "Overall Illion coverage (all rows)"),
        "short": bi("整体 illion 覆盖率", "Overall Illion coverage"),
        "unit": "rate", "src": "illion_coverage", "global": "illion_coverage",
        "num_label": bi("illion 已分类", "Illion classified"),
        "den_label": bi("全量样本", "All rows"),
        "global_only": True},
    "overall_iou_rate": {
        "label": bi("整体一致率（并集口径）", "Overall agreement (union)"),
        "short": bi("整体一致率", "Overall agreement"),
        "unit": "rate", "src": "coverage_adjusted_agreement", "global": "agreement",
        "num_label": bi("两侧标签相同", "Matching labels"),
        "den_label": bi("并集", "Union"),
        "global_only": True},
    # global 指向 ① 指标卡里那个数（build_metrics 的键）。有的指标没有 src
    # （unknown_rate 没有「某类某天」的列，只判整体），有的两个都有（overall_*）。
    # 加载时按方向挡：类别规则不能用只有 global 的指标，全局规则不能用没有 global
    # 的指标。全局规则命中的展示位是 ① 指标卡角标，由配置里的 tile 指定
    # （见 TILE_METRIC_KEYS）——命中了却看不见的待办比不报还糟。
}

# 判据支持的比较方式。gt_baseline / gte_baseline 是「与历史基准比」，
# 基准 = 往前 window 天的中位数，不足 min_days 天时判为「数据不足」而不是不触发也不放过。
CLAUSE_OPS = ("gt", "gte", "lt", "lte", "gt_baseline", "gte_baseline")

RULES_CONFIG_FILE = "rules_config.json"

# 规则表由 load_rules_config 在 main 开头填。写成模块级是沿用原来的形状：
# rule_thresholds / rules_table / evaluate_rules 都读这一份，改配置重跑一次即生效，
# 不必把配置对象沿着调用链一路传下去。
MONITORING_RULES: list[dict] = []
ALERT_RULES: list[dict] = []
RULE_BY_ID: dict[str, dict] = {}

# 全局规则（scope=global）的待办不属于任何类别，统一归到这个名字下。
# 用中文名而不是空串：它会出现在卡片标题和待办列表里，得看得懂。
GLOBAL_CATEGORY = "全体类别"

# 变化检测的两个阈值：
#   Stable 带——|Δ| 小于 0.05 pt 落进「Δ 显示到 0.1 pt」的精度以下，表格里显示
#     0.0 pt 并记 Stable。这是沿用页脚既有的约定：舍掉的变化不报。
#   走坏带——指标往坏方向走满 1.0 pt 才算「真的走坏了」（changes 行的 worsened）。
#     它与规则无关：规则管的是「要不要开工单」，它管的是「这一项今天值不值得多看一眼」。
CHANGE_STABLE_PP = 0.05
CHANGE_BAD_PP = 1.0

# 变化检测的七行。dir 说明「哪个方向算坏」：两侧覆盖率是 neutral——
# 覆盖率涨了不一定是好事（可能是把边缘样本也拉进来了），所以它判不了走坏，
# 只报方向与幅度，读的人自己判断。
#
# 页面上不再摆这七行（原来那张 Current / Previous / Delta 表已经删掉）。它们现在只喂
# 两处：worsened 的定档，以及 ② 图表下面那一句变化摘要——摘要只说越线的那几个，
# 所以「走坏」的判据仍然只有这一份。
METRIC_DEFS = [
    ("agreement",       bi("一致率",         "Agreement"),        "coverage_adjusted_agreement", "up_good"),
    ("bscat_coverage",  bi("BSCAT 覆盖率",   "BSCAT coverage"),   "finv_coverage",               "neutral"),
    ("illion_coverage", bi("Illion 覆盖率",  "Illion coverage"),  "illion_coverage",             "neutral"),
    ("difference",      bi("差异率",         "Difference rate"),  "all_difference_rate",         "down_good"),
    ("miss",            bi("漏识别率",       "Miss rate"),        "miss_rate",                   "down_good"),
    ("extra",           bi("多识别率",       "Extra rate"),       "extra_rate",                  "down_good"),
    ("unknown",         bi("未分类率",       "Unclassified rate"), "unknown_rate",               "down_good"),
]

# 明细只用来算聚合：类别 → 差异类型 → 差异流向的笔数与金额。页面不再内嵌逐笔
# 交易示例（2026-11-13 用户定），所以流向上不再挂 samples，页面体积与样本量无关。


def log(msg: str) -> None:
    print(f"[bscat] {msg}", flush=True)


# ── 读取 ──────────────────────────────────────────────────────────────────
def load(input_dir: Path, name: str) -> pd.DataFrame:
    path = input_dir / FILES[name]
    if not path.exists():
        raise SystemExit(f"缺少输入文件: {path}")
    return pd.read_csv(path, encoding="utf-8-sig")


# 取数件的日期列，上游换过名字：早期叫 par_dt，2026-10 起叫 sample_datetime，而且从
# 第二列挪到了最后一列。两个名字都认，在这里翻译成 par_dt——趋势行、上一期
# 查找全都按 par_dt 认人，只在这一处认两种名字，好过让每个用到日期的地方各认一遍
# （认漏一处就是一次静默的空值，而不是报错）。列名都还在、整列却是空的时候同样算取不到：
# 那种表拿来算历史，会往库里写进一行日期为空的记录，比当场停下难查得多。
INPUT_DATE_COLS = ("par_dt", "sample_datetime")


def normalize_dates(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """把一份取数件的日期列统一成 par_dt，供按时切片与历史归并使用。"""
    for col in INPUT_DATE_COLS:
        if col not in df.columns:
            continue
        vals = [str(v).strip() for v in df[col].tolist()]
        if any(v and v != "nan" for v in vals):
            df = df.copy()
            df["par_dt"] = vals
            return df
    raise SystemExit(
        f"{name} 表里没有可用的监控日期列：认的列名是 {list(INPUT_DATE_COLS)}，"
        f"实际是 {list(df.columns)}。上游再改列名时，把它加进 INPUT_DATE_COLS。")


# ── 多日批量（2026-10 起）─────────────────────────────────────────────────
# 取数件从这一批起一次导出多天（当期这批 18 天），而且**行序不是日期序**：趋势表的
# 最后一行是中间某天（trend 尾行 10-06、当期其实是 10-08），summary / category /
# group 的最后一个日期块是最旧的那天。所以「当期」只能取日期列的最大值，不能按行取；
# 四份表进聚合之前都要按天切片——build_summary（按键覆盖）、build_categories（逐行
# 铺开）这类整表照单全收的函数在多日输入下不报错，只会把中间某天的数字印成最新一期，
# 或者让某一期的值被另一天覆盖，两种都查不出来。
def batch_days(df: pd.DataFrame) -> list[str]:
    """一份取数件覆盖的日期，升序。"""
    return sorted({str(x) for x in df["par_dt"]})


def rows_on(df: pd.DataFrame, d: str, name: str) -> pd.DataFrame:
    """按天切片。缺哪一天就停在哪一天：静默少一天等于把另一天的数印成这一天的。"""
    sub = df[df["par_dt"].astype(str) == str(d)]
    if sub.empty:
        raise SystemExit(f"{name} 表里没有 {d} 这一天的行——取数件少了一天？")
    return sub


# detail 表实际用到的列。整表 24 列 / 780 万行，全读进来本机 16 GB 装不下，只能按天
# 分片过（见 detail_days），列也要挑着读——列越少，峰值内存越低。
# 剩下的列（是否关键Category、transaction_id / job_id、*_原始、差异流向数量、
# 逐笔示例用的日期 / 收付 / 对手方 / 申请号 / 账号）没有一处读它们：类别 P 档由板块 +
# 点名算（见 cat_level），流向数量从明细现算，逐笔示例已不再内嵌。
# 「排查优先级」2026-11-13 起上游也不再输出（P 档只由本文件这一套定）。
DETAIL_COLS = ["排查类型", "Category比对状态", "illion_Category", "finv_Category",
               "差异流向", "amount", "user_id", "sample_datetime"]
# 这几列上游混着写数字和文本（类别名、流向标签），让 pandas 按列推理会在每个分片上
# 各推一次，推出两种 dtype 再警告一遍。统一按文本读。
DETAIL_TEXT_COLS = [c for c in DETAIL_COLS if c != "amount"]
DETAIL_CHUNK = 200_000


def detail_days(path: Path):
    """
    按天产出明细行（一天一份 DataFrame，块序 = 文件里的顺序，不保证日期序）。

    明细是唯一大到不能整表进内存的一份，而它按天分块（同一天的行连在一起），所以
    流式读、攒够一天把那天交出去，峰值只压着一天。分块这个前提不成立时（某一天的行
    散落在两处）当场停下：再往下算就是把同一天并两遍，而结果看着完全正常。
    """
    buf: list[pd.DataFrame] = []
    cur: str | None = None
    done: set[str] = set()
    for ch in pd.read_csv(path, encoding="utf-8-sig", chunksize=DETAIL_CHUNK,
                          usecols=DETAIL_COLS, dtype={c: str for c in DETAIL_TEXT_COLS}):
        day_of = ch["sample_datetime"].astype(str).str.strip()
        for d, sub in ch.groupby(day_of, sort=False):
            if d != cur:
                if cur is not None:
                    yield cur, pd.concat(buf, ignore_index=True)
                    done.add(cur)
                    buf = []
                if d in done:
                    raise SystemExit(
                        f"明细表里 {d} 这一天的行不连续（分成两块以上）——按天分片"
                        f"的前提没了，再往下算会把同一天并两遍。请先按日期块重排取数件。")
                cur = d
            buf.append(sub)
    if buf and cur is not None:
        yield cur, pd.concat(buf, ignore_index=True)


def is_empty(v) -> bool:
    """判断分类值是否为空（含 '(空)' 这类占位符）。"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return True
    return str(v).strip() in EMPTY_TOKENS


def show(v) -> str:
    return EMPTY_LABEL if is_empty(v) else str(v).strip()


def num(v) -> float:
    """源表在某一侧没有交易时金额列是 NaN（如 Unknown Loans 的 illion金额），按 0 处理。"""
    return float(v) if pd.notna(v) else 0.0


# ── 趋势行 ────────────────────────────────────────────────────────────────
def plan_trend(trend_df: pd.DataFrame, extras: dict[str, dict]) -> tuple[pd.DataFrame, list[dict]]:
    """
    把取数件里的趋势行整理成页面记录，返回（按日期排序的表, 页面记录）。

    extras 是**按天**的一份映射（日期 -> 那一天的派生列）。决策列由 summary 现算，
    而 summary 也是多天的：拿当期的派生值铺给整批行，等于让 18 天共用一天的
    unknown_rate，图上那几条线会一起变成同一条。
    """
    hist = trend_df[TREND_SRC_COLS].copy()
    days = [str(x) for x in hist["par_dt"]]
    for k in TREND_EXTRA_COLS:
        hist[k] = [extras.get(d, {}).get(k) for d in days]

    hist = hist.sort_values("par_dt").reset_index(drop=True)

    keep = ["par_dt", "total_rows", "union_rows", "both_empty_rows", "both_class_rows",
            "illion_coverage", "finv_coverage",
            "coverage_adjusted_agreement", "coverage_overlap_rate", "miss_rate",
            "extra_rate", "unknown_rate", "all_difference_count", "all_difference_rate",
            # 漏识别 / 多识别的**笔数**：① 的 miss / extra 两张卡写的是「N 笔 ÷ 并集」，
            # 只有率是凑不出那个 N 的（率乘分母再取整会在末位飘），所以这两列也得随载荷走。
            "illion_only_count", "finv_only_count",
            # 三个分子：历史行缺这三列时是 NaN，to_json 会写成 null，页面显示「—」。
            # 不在这里补 0——0 会被读成「那天一个都没对上」。
            "illion_coverage_num", "finv_coverage_num", "agreement_num",
            # 差异金额：更早的历史行没有这一列，null 在图上就是断开的一段
            "mismatch_amount"]
    return hist, json.loads(hist[keep].to_json(orient="records", force_ascii=False))


def prev_category_snapshot(hist: pd.DataFrame, data_date: str) -> dict:
    """
    上一期各类别的快照。上一期 = data_date 之前最近的那一期；没有就返回空字典，
    调用方据此把 Δ 全部留空——首期没有变化可言，拿 0 冒充会读成「没有变化」。
    """
    if hist.empty:
        return {}
    prev_dates = hist.loc[hist["par_dt"].astype(str) < data_date, "par_dt"]
    if prev_dates.empty:
        return {}
    prev = hist[hist["par_dt"].astype(str) == str(prev_dates.max())].set_index("name")
    return json.loads(prev.to_json(orient="index", force_ascii=False))


def plan_category_history(hist: pd.DataFrame, cats: list[dict], data_date: str
                          ) -> tuple[pd.DataFrame, int]:
    """
    把各类别的当期指标并进历史（按 par_dt + 类别去重覆盖），返回（合并后, 本期行数）。
    """
    today = pd.DataFrame([{
        "par_dt": data_date,
        "name": c["name"],
        "group": c["group"],
        "illion_count": c["illion_count"],
        "finv_count": c["finv_count"],
        "agree_count": c["agree_count"],
        "union_count": c["union_count"],
        "iou_rate": c["iou_rate"],
        "illion_coverage": c["illion_coverage"],
        "finv_coverage": c["finv_coverage"],
        "output_share": c["output_share"],
        "diff_count": c["diff_count"],
        "diff_amount": c["diff_amount"],
        "miss_count": c["miss_count"],
        "extra_count": c["extra_count"],
    } for c in cats])

    if not hist.empty:
        merged = pd.concat([hist, today], ignore_index=True)
        merged["par_dt"] = merged["par_dt"].astype(str)
        merged = merged.drop_duplicates(subset=["par_dt", "name"], keep="last")
    else:
        merged = today
    merged = merged.sort_values(["par_dt", "name"]).reset_index(drop=True)
    return merged, int((merged["par_dt"] == data_date).sum())


def delta(cur, prev_row: dict | None, key: str) -> float | None:
    """当期值 − 上一期值。任一侧取不到（首期、或那期该字段为空）就返回 None。"""
    if prev_row is None or cur is None:
        return None
    pv = prev_row.get(key)
    if pv is None or pd.isna(pv):
        return None
    return float(cur) - float(pv)


# ── 聚合 ──────────────────────────────────────────────────────────────────
def build_summary(sm: pd.DataFrame) -> dict:
    out: dict = {}
    for _, r in sm.iterrows():
        key = SUMMARY_KEYS.get(int(r["ord"]))
        if not key:
            continue
        out[key] = float(r["结果"]) if pd.notna(r["结果"]) else None
        # 分子/分母一并带上，页面上要拿原始笔数写口径说明（如覆盖率差 = 两侧笔数之差）
        if pd.notna(r["分子"]):
            out[f"{key}_num"] = float(r["分子"])
        if pd.notna(r["分母"]):
            out[f"{key}_den"] = float(r["分母"])
    return out


def trend_extra(s: dict) -> dict:
    """
    趋势源表里没有、按 summary 现算的决策列。

    前几个率统一以并集（至少一侧给出分类）为分母，这样「漏识别率 + 多识别率 +
    边界差异率 = 1 − 交集÷并集」，可以直接横向比、也能互相加减，
    不像覆盖率那样受两侧口径差影响。

    unknown_rate 是唯一的例外：双方都无分类的交易不在并集里，分母只能用**全量样本**。
    口径不同就不参与上面那套加减，页脚里单独写明了。
    """
    union = s.get("coverage_adjusted_agreement_den")
    illion_only, finv_only = s.get("illion_only"), s.get("finv_only")
    total, both_empty = s.get("total"), s.get("both_empty")
    return {
        "union_rows": union,
        "both_class_rows": s.get("coverage_overlap_rate_num"),
        "both_empty_rows": both_empty,
        "coverage_overlap_rate": s.get("coverage_overlap_rate"),
        "miss_rate": round(illion_only / union, 6) if union and illion_only is not None else None,
        "extra_rate": round(finv_only / union, 6) if union and finv_only is not None else None,
        "unknown_rate": (round(both_empty / total, 6)
                         if total and both_empty is not None else None),
        # 三个分子直接搬 summary 的原值，不重算
        "illion_coverage_num": s.get("illion_coverage_num"),
        "finv_coverage_num": s.get("finv_coverage_num"),
        "agreement_num": s.get("coverage_adjusted_agreement_num"),
        # 差异金额不在 summary 里，由 main 算好以后补进来（要 detail 表，
        # trend_extra 只拿得到 summary）。补不进来就留空，不在这里编一个数。
        "mismatch_amount": None,
    }


def build_mismatch(detail: pd.DataFrame) -> list[dict]:
    counts = detail["排查类型"].value_counts().to_dict()
    out = []
    for key, label, color, desc, alias in MISMATCH_TYPES:
        n = int(counts.get(alias, 0))
        if key == "diff":  # flow 表口径，防止 detail 侧改词导致漏计
            n = n or int(detail["Category比对状态"].value_counts().get("分类不一致", 0))
        out.append({"key": key, "label": label, "color": color,
                    "desc": desc, "alias": alias, "count": n,
                    # 受控值：label 是给人看的中文，enum 是给统计用的受控值。
                    # 两者并存而不是二选一——中文标签已经在页面上用了很久，
                    # 换成英文枚举值等于改视觉，而受控值不给出又没法做长期统计。
                    "enum": DIFF_TYPE_ENUM[key]})
    return out


def build_segments(gr: pd.DataFrame, rows: list[dict], sample_total: int) -> list[dict]:
    """
    业务板块 -> 成员 Category，成员名单来自 group 表的「成员Category」列。
    板块汇总直接由 category 行加总得出（不用 group 表自带的汇总列），
    这样分区小计和表格里逐行相加的结果必然一致。

    返回顺序沿用 group 表的行序（收入类 / 支出类 / 贷款类 / 转账类），第三部分的
    分块顺序直接用它：按差异笔数重排会让板块位置随数据漂移，读者每次都要重新找一遍。
    """
    by_name = {r["name"]: r for r in rows}
    segs: list[dict] = []
    for _, r in gr.iterrows():
        name = str(r["业务大类"]).strip()
        if name == "合计":
            continue
        members = [m.strip() for m in str(r["成员Category"]).split(";")
                   if m.strip() and m.strip() != "-"]
        members = [m for m in members if m in by_name]
        if not members:
            continue          # 「未分类」这类空板块直接跳过
        for m in members:
            by_name[m]["group"] = name

        agg = lambda k: sum(by_name[m][k] for m in members)
        agg2 = lambda k: sum(by_name[m]["type_mix"][k] for m in members)
        illion_n, finv_n, agree_n = agg("illion_count"), agg("finv_count"), agg("agree_count")
        diff_n, diff_amt = agg("diff_count"), agg("diff_amount")
        segs.append({
            "key": name,
            "member_count": len(members),
            "illion_coverage": illion_n / sample_total if sample_total else 0.0,
            "finv_coverage": finv_n / sample_total if sample_total else 0.0,
            "coverage_gap": (finv_n - illion_n) / sample_total if sample_total else 0.0,
            "agree_rate": (agree_n / illion_n) if illion_n else None,
            "agree_share": agree_n / sample_total if sample_total else 0.0,
            "diff_count": diff_n,
            "diff_amount": diff_amt,
            "share": 0.0,          # 板块内占比，等一下用板块合计口径回填
            # 小计行也要能显示两列决策信息，口径与逐行相加一致
            "recover_pt": agg("recover_pt"),
            "type_mix": {k: agg2(k) for k, *_ in MISMATCH_TYPES},
        })

    seg_total = sum(s["diff_count"] for s in segs) or 1
    for s in segs:
        s["share"] = s["diff_count"] / seg_total
    # 不在这里排序：第三部分按这个顺序分块，必须稳定为 group 表的行序
    return segs


def cat_level(name: str, group: str | None) -> str:
    """一个类别的 P 档。定义见文件头的 LEVEL_BY_SEGMENT / LEVEL_BY_NAME。

    板块优先于点名：同一个类别既在 P0 板块里、又被点名，按板块算（自检会把这种
    重叠报出来，不靠这里的先后顺序悄悄化解）。
    """
    if group in LEVEL_BY_SEGMENT:
        return LEVEL_BY_SEGMENT[group]
    return LEVEL_BY_NAME.get(name, "P2")


def assign_levels(cats: list[dict]) -> None:
    """把 P 档写进每一行。"""
    for c in cats:
        c["level"] = cat_level(c["name"], c.get("group"))


# ── 运行上下文与运维记录 ──────────────────────────────────────────────────
# 这两样东西随「一次运行 = 一批样本 + 一个 Run + 一个模型版本」一起退役了，见文件
# 顶部的「每日监控」注释。原来在这里的 normalize_maintenance_action / load_run_context
# / load_maintenance / comparison_kind / prev_run_context 全部删除：没有数据源，
# 也没有可以倒推出批次或 Run 的地方。
# ── 监控规则引擎 ──────────────────────────────────────────────────────────
# 规则决定「要不要生成运维待办、生成哪一类」。这里的产出就是最终待办，
# 页面不再参与筛选，也没有第二条路能造出一条待办。
def format_condition(rule: dict, lang: str = "zh") -> str:
    """把条件文案里的 {阈值} 占位换成实际值，保证「写的」和「判的」是同一组数字。

    lang 决定读配置里哪一份文案：zh 读 condition，en 读 en.condition。两份的占位符
    在加载配置时校验过逐字相同，所以同一份取值表对两边都成立——不存在「中文写 60%、
    英文写 0.6」这种分叉。
    """
    tpl = rule["condition"] if lang == "zh" else rule["en"]["condition"]
    return tpl.format(**rule_thresholds_of(rule))


def cond_pair(rule: dict) -> dict:
    """判据文案成对。两条走同一个 rule_thresholds_of，数字只有一份。"""
    return bi(format_condition(rule, "zh"), format_condition(rule, "en"))


def clause_key(cl: dict) -> str:
    """一条判据在 condition 文案里的占位符名字。默认与指标同名，重名时才写 as。"""
    return str(cl.get("as") or cl["metric"])


def rule_thresholds_of(rule: dict) -> dict:
    """
    单条规则的取名表：判据的阈值 / 倍数，外加规则级的连续天数。

    window 只在文案里引用了 {window} 时才带上——每条规则各有一个 window，
    打平成一整份时会互相覆盖，所以它只活在规则内部。
    """
    out = {clause_key(cl): (cl["ratio"] if "ratio" in cl else cl["threshold"])
           for cl in rule_clauses(rule)}
    # 两份文案都看一眼：加载时的校验要求它们占位符同号，所以这两个条件是同一件事，
    # 写成 or 只是不让「配置校验没跑到的那条路」上出现一份缺 window 的取值表。
    tpls = (rule["condition"], (rule.get("en") or {}).get("condition") or "")
    if any("{window}" in t for t in tpls):
        out["window"] = rule.get("window", 1)
    return out


def rule_clauses(rule: dict) -> list[dict]:
    """一条规则的判据列表。all / any 二选一，形态在加载时已经校验过。"""
    trg = rule["trigger"]
    return trg.get("all") or trg.get("any") or []


def rule_mode(rule: dict) -> str:
    return "all" if "all" in rule["trigger"] else "any"


def rule_thresholds() -> dict:
    """
    打平的阈值字典，给页面按名取用（JS 里的 R.iou_rate）。

    只装判据的阈值，不装 window：连续天数每条规则各有一个，打平必然互相覆盖，
    最后一个赢——那种「看起来取到了、其实是别人的值」的字典比没有更糟。
    每条规则自己的 window 走 rulesTable 里的 thresholds。
    """
    return {clause_key(cl): (cl["ratio"] if "ratio" in cl else cl["threshold"])
            for r in (MONITORING_RULES + ALERT_RULES) for cl in rule_clauses(r)}


# ── 规则配置：rules_config.json ──────────────────────────────────────────
def load_rules_config(path: Path) -> tuple[list[dict], list[dict]]:
    """
    读规则配置并逐项校验，返回（当期规则, 按日规则）。

    配置是运行期输入，写错了必须在**开跑之前**停住：一条判据说不出根因的规则，
    在页面上表现为「页脚写着这条规则、但从来不触发」，没有任何别的症状。
    所以这里查出问题就 SystemExit，把问题一次列全，而不是让脚本带着半份规则跑完。

    配置里不认的键一律报出来（多打一个字母的键会被静默忽略，而它的症状是
    「我明明改了这个阈值，怎么没生效」），只放行 $doc 与 _ 开头的注记键。
    """
    if not path.exists():
        raise SystemExit(f"缺少规则配置文件: {path}")
    try:
        cfg = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise SystemExit(f"规则配置读不出来: {path}（{e}）")

    bad: list[str] = []
    if not isinstance(cfg, dict):
        raise SystemExit(f"规则配置的顶层必须是一个对象: {path}")
    for k in cfg:
        if k not in ("version", "snapshot", "alerts") and not k.startswith(("$", "_")):
            bad.append(f"顶层多出的键 {k!r}（注记请用 $ 或 _ 开头）")
    if not isinstance(cfg.get("version"), int):
        bad.append("缺少整数 version")

    rule_keys = {"id", "enabled", "scope", "tile", "title", "window", "condition", "trigger",
                 "context", "metrics", "issueType",
                 "recommendedAction", "suppressedBy", "actionHint", "en"}
    clause_keys = {"metric", "op", "threshold", "ratio", "window", "min_days", "as"}
    metric_keys = {k for k, *_ in METRIC_DEFS}
    seen_ids: set[str] = set()
    out: dict[str, list[dict]] = {"snapshot": [], "alerts": []}

    for section in ("snapshot", "alerts"):
        rules = cfg.get(section)
        if rules is None:
            rules = []
        if not isinstance(rules, list):
            bad.append(f"{section} 必须是数组")
            continue
        for raw in rules:
            if not isinstance(raw, dict):
                bad.append(f"{section} 里有一条不是对象: {raw!r}")
                continue
            rid = str(raw.get("id") or "")
            where = f"{section}/{rid or '?'}"
            for k in raw:
                if k not in rule_keys:
                    bad.append(f"{where}: 多出的键 {k!r}")
            if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", rid):
                bad.append(f"{where}: 规则 ID 只能由字母数字下划线组成")
            elif rid in seen_ids:
                bad.append(f"{where}: 规则 ID 重复")
            seen_ids.add(rid)
            if not str(raw.get("title") or "").strip():
                bad.append(f"{where}: 缺 title")
            if raw.get("scope") not in ("category", "global"):
                bad.append(f"{where}: scope {raw.get('scope')!r} 不是 category / global")
            # 两段规则的分工：snapshot 只支持 category；alerts 支持 category / global。
            # 全局按日规则命中后挂在 ① 指标卡的角标上，所以必须写 tile——一条命中了
            # 却在页面上哪儿都看不见的待办比不报还糟。
            # 2026-10-14 / 10-16 两次试图给全局命中单独开一块，都当天撤掉了；角标是
            # 第三次，挂在读数旁边，不新增区块。
            if section == "snapshot" and raw.get("scope") != "category":
                bad.append(f"{where}: 当期快照规则只支持 scope=category")
            tile = raw.get("tile")
            if raw.get("scope") == "global":
                if not str(tile or "").strip():
                    bad.append(f"{where}: 全局规则必须写 tile（命中后挂在哪张 ① 指标卡上）")
                elif tile not in TILE_METRIC_KEYS:
                    bad.append(f"{where}: tile {tile!r} 不在 ① 指标卡名单 "
                               f"{list(TILE_METRIC_KEYS)} 里")
            elif tile is not None:
                bad.append(f"{where}: 只有全局规则写 tile，类别规则没有角标位")
            if not isinstance(raw.get("enabled", True), bool):
                bad.append(f"{where}: enabled 必须是 true / false")
            win = raw.get("window", 1)
            if not isinstance(win, int) or isinstance(win, bool) or win < 1:
                bad.append(f"{where}: window 必须是 >= 1 的整数（当前 {win!r}）")
            if raw.get("issueType") not in ENUMS["issueType"]:
                bad.append(f"{where}: issueType {raw.get('issueType')!r} 不在枚举里")
            if raw.get("recommendedAction") not in ENUMS["recommendedAction"]:
                bad.append(f"{where}: recommendedAction {raw.get('recommendedAction')!r} 不在枚举里")
            if not set(raw.get("metrics") or []) <= metric_keys:
                bad.append(f"{where}: metrics {raw.get('metrics')} 里有不存在的指标行")
            for s in raw.get("suppressedBy") or []:
                if s not in {r.get("id") for r in rules} | seen_ids:
                    bad.append(f"{where}: 压制的规则 {s!r} 不存在")
            ctx = raw.get("context") or []
            if not isinstance(ctx, list) or not set(ctx) <= set(ALERT_METRICS):
                bad.append(f"{where}: context {ctx!r} 里有不在告警指标名单里的名字")

            trg = raw.get("trigger")
            clauses: list[dict] = []
            if not isinstance(trg, dict) or set(trg) - {"all", "any"} or len(
                    [k for k in ("all", "any") if k in trg]) != 1:
                bad.append(f"{where}: trigger 必须是 {{\"all\": [...]}} 或 {{\"any\": [...]}} 之一")
            else:
                mode = "all" if "all" in trg else "any"
                clauses = trg[mode]
                if not isinstance(clauses, list) or not clauses:
                    bad.append(f"{where}: trigger.{mode} 是非空数组")
                    clauses = []
                for cl in clauses:
                    if not isinstance(cl, dict):
                        bad.append(f"{where}: 判据不是对象: {cl!r}")
                        continue
                    for k in cl:
                        if k not in clause_keys:
                            bad.append(f"{where}: 判据里多出的键 {k!r}")
                    mk = cl.get("metric")
                    if mk not in ALERT_METRICS:
                        bad.append(f"{where}: 指标 {mk!r} 不在告警指标名单里")
                        continue
                    if raw.get("scope") == "global" and "global" not in ALERT_METRICS[mk]:
                        bad.append(f"{where}: 全局规则用了没有全局值的指标 {mk!r}")
                    # 反向也要挡：只有 global 的指标（整体一致率等）在类别序列里没有列，
                    # 类别规则引用它，运行期取数会取到空序列，报一串调用栈而不是「写错了」
                    if raw.get("scope") == "category" and not ALERT_METRICS[mk].get("src"):
                        bad.append(f"{where}: 类别规则用了只有全局值的指标 {mk!r}")
                    # 整体域指标两个来源都有，不挡的话一条类别规则写错名字会静默地
                    # 按「本类覆盖率」判，页面上完全看不出它判错了域。
                    if raw.get("scope") == "category" and ALERT_METRICS[mk].get("global_only"):
                        bad.append(f"{where}: 类别规则不能使用整体指标 {mk!r}")
                    if cl.get("op") not in CLAUSE_OPS:
                        bad.append(f"{where}: 比较方式 {cl.get('op')!r} 不在 {CLAUSE_OPS} 里")
                    base = str(cl["op"]).endswith("baseline")
                    num = cl.get("ratio") if base else cl.get("threshold")
                    if not isinstance(num, (int, float)) or isinstance(num, bool):
                        bad.append(f"{where}: 判据 {'ratio' if base else 'threshold'} "
                                   f"必须是数字（当前 {num!r}）")
                    elif base:
                        bw = cl.get("window")
                        bm = cl.get("min_days")
                        if not isinstance(bw, int) or isinstance(bw, bool) or bw < 1:
                            bad.append(f"{where}: 基准窗口 window 必须是 >= 1 的整数")
                        if not isinstance(bm, int) or isinstance(bm, bool) or bm < 1:
                            bad.append(f"{where}: min_days 必须是 >= 1 的整数")
                        elif isinstance(bw, int) and bm > bw:
                            bad.append(f"{where}: min_days {bm} 比基准窗口 {bw} 还大，永远凑不够")

            keys = [clause_key(cl) for cl in clauses if isinstance(cl, dict)]
            dup = sorted({k for k in keys if keys.count(k) > 1})
            if dup:
                bad.append(f"{where}: 判据占位符重名 {dup}，重名的判据要写 as 区分")
            holes = set(re.findall(r"\{(\w+)(?::[^}]*)?\}", str(raw.get("condition") or "")))
            if holes != set(keys) | ({"window"} if "{window}" in str(raw.get("condition")) else set()):
                bad.append(f"{where}: 条件占位符 {sorted(holes)} 与判据 {sorted(keys)} 不一致")
            if "window" in holes and win < 1:
                bad.append(f"{where}: 文案里引用了 {{window}}，window 必须 >= 1")

            # ── 英文那一半 ────────────────────────────────────────────────
            # 英文不是可选装饰：缺 en 块、或 en 少一个键，这里直接中止（用户
            # 2026-10-16 定「漏译直接报错中止」）。配置是手改的、进版本库的，漏译
            # 就是配置写坏了；放过去的症状是英文页面上静默冒出一句中文，比报错难查。
            # 代价也是明说的：「脚本升了级、规则没补 en」那一跑会失败、当天产物不产出。
            en = raw.get("en")
            if not isinstance(en, dict):
                bad.append(f"{where}: 缺 en 块（title / condition / actionHint）")
            else:
                for k in ("title", "condition", "actionHint"):
                    if not str(en.get(k) or "").strip():
                        bad.append(f"{where}: en 缺 {k}")
                for k in en:
                    if k not in ("title", "condition", "actionHint"):
                        bad.append(f"{where}: en 里多出的键 {k!r}")
                # 占位符必须与中文那份**逐字相同**：两份文案引用的必须是同一组阈值。
                # 各写各的话，中文页写 60%、英文页写 0.6，而自检只看中文那一份，
                # 错的那一半没有任何地方会喊。
                en_holes = set(re.findall(r"\{(\w+)(?::[^}]*)?\}",
                                          str(en.get("condition") or "")))
                if en_holes != holes:
                    bad.append(f"{where}: en.condition 的占位符 {sorted(en_holes)} "
                               f"与中文的 {sorted(holes)} 不一致")

            rule = dict(raw)
            rule["window"] = win
            rule["enabled"] = bool(raw.get("enabled", True))
            rule["suppressedBy"] = list(raw.get("suppressedBy") or [])
            rule["context"] = list(raw.get("context") or [])
            rule["thresholds"] = rule_thresholds_of(rule)
            out[section].append(rule)

    if bad:
        raise SystemExit("规则配置有问题，已中止:\n  - " + "\n  - ".join(bad))
    return out["snapshot"], out["alerts"]


def set_rules(snapshot: list[dict], alerts: list[dict]) -> None:
    """把校验过的规则表装上，并重建按 ID 索引。规则表只有这一处来源。"""
    global MONITORING_RULES, ALERT_RULES, RULE_BY_ID
    MONITORING_RULES = snapshot
    ALERT_RULES = alerts
    RULE_BY_ID = {r["id"]: r for r in snapshot + alerts}


# ── 判据求值 ──────────────────────────────────────────────────────────────
def _transform(key: str, v) -> float | None:
    """
    把一个源值变成判据值。变换只有两种（pt 类 ×100、abs 类取绝对），只在这一处做：
    判据、卡片、页面上那条线取的都是它，各写一遍迟早会出现「图上 3 pt、规则按 0.03 判」。

    取不到一律返回 None，绝不用 0 顶替：0 会被读成「测到了，是零」，
    而 None 一路走到页面上是「数据不足」——两件事。
    """
    if v is None:
        return None
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v):
        return None
    if ALERT_METRICS[key].get("pt"):
        v *= 100.0
    return abs(v) if ALERT_METRICS[key].get("abs") else v


def metric_value(row: dict | None, key: str) -> float | None:
    """
    从一个「某类某天」的行里取告警指标的判据值。

    全局域规则传进来的行由 global_row 造，形状与类别行一致，
    所以这里不必知道自己在判哪一域。

    取数的键名两域不同：类别行走 src（列名），全局行走 global（metrics 里的键）。
    原来只认 src，于是「全局行只有 global 键」的指标取出来恒为 None——RULE_003
    因此从来没触发过，页面上却看不出任何异样（不触发与正常长得一样）。
    """
    if row is None:
        return None
    d = ALERT_METRICS[key]
    return _transform(key, row.get(d.get("src") or d.get("global")))


def metric_text(key: str, v: float | None) -> dict:
    """指标值的展示文案，成对返回。四种单位各有各的写法，页面不重算。

    取不到值印「数据不足」而不是 0：0 是「测到了，是零」，两件事（同一页面上
    还有一处同样的判断，见 _transform）。
    """
    if v is None:
        return bi("数据不足", "no data")
    unit = ALERT_METRICS[key]["unit"]
    if unit == "rate":
        return unipair(f"{v * 100:.2f}%")
    if unit == "pt":
        return unipair(f"{v:+.2f} pt")
    if unit == "money":
        return unipair(money(v))
    return bi(f"{int(round(v)):,} 笔", f"{int(round(v)):,} rows")


def _clause_hit(cl: dict, vals: list, k: int) -> bool | None:
    """
    一条判据在第 k 天成立与否。None = 判不出来（这一天缺值 / 基准还没攒够）。

    「判不出来」必须是第三种状态：把它当 false 会把「不知道」说成「正常」，
    当 true 会凭空造出一条预警。
    """
    v = vals[k]
    if v is None:
        return None
    op = cl["op"]
    if op.endswith("baseline"):
        w, mn = cl["window"], cl["min_days"]
        hist = [x for x in vals[max(0, k - w):k] if x is not None]
        if len(hist) < mn:
            return None
        base = statistics.median(hist)
        # 基准是 0（这一类历史上没有差异）时倍数没有意义，判不了，不猜
        if base <= 0:
            return None
        return v > base * cl["ratio"] if op == "gt_baseline" else v >= base * cl["ratio"]
    th = cl["threshold"]
    return {"gt": v > th, "gte": v >= th, "lt": v < th, "lte": v <= th}[op]


def _day_result(rule: dict, vals_of: dict[str, list], k: int) -> bool | None:
    """规则在第 k 天的结果：True / False / None（判不出来）。"""
    res = [_clause_hit(cl, vals_of[clause_key(cl)], k) for cl in rule_clauses(rule)]
    if rule_mode(rule) == "all":
        if any(r is False for r in res):
            return False
        return None if any(r is None for r in res) else True
    if any(r is True for r in res):
        return True
    return None if any(r is None for r in res) else False


def rule_streak(rule: dict, vals_of: dict[str, list], end_k: int) -> dict:
    """
    从 end_k 往回数，判据连续成立了几天。

    只数「连续成立」的那一段：中间断一天（判据不成立，或那天缺值判不出来）
    就到此为止。缺值**不跳过**——跳过去等于把「不知道那天怎么样」算成
    「那天也异常」，连续天数会凭空变长。
    """
    days, k = 0, end_k
    while k >= 0 and _day_result(rule, vals_of, k) is True:
        days += 1
        k -= 1
    return {"today": _day_result(rule, vals_of, end_k), "days": days}


def rule_history_need(rule: dict) -> dict:
    """这条规则要几天历史才判得出来。算不出基准时按这个数说明「还差几天」。"""
    streak_need = rule.get("window", 1)
    base_need = max([cl["min_days"] for cl in rule_clauses(rule)
                     if str(cl["op"]).endswith("baseline")] or [0])
    return {"streak": streak_need, "baseline": base_need}


def metric_keys_of(rule: dict) -> list[str]:
    """这条规则用到哪几个**告警指标**：按判据在文案里的出现顺序排列、去重。

    注意与 clause_key 不是一回事：一条规则里同一个指标可以出现两次（「差异笔数超过
    1500 **或**超过历史中位数」，两个判据同指标、阈值不同），那时第二个判据写 as
    给占位符换个名字，但指标还是同一个——展示时不能把「差异笔数」印两遍。

    顺序就是 condition 里那串指标的顺序，于是证据行「一致率 61.25% · 覆盖率差 +32.63 pt」
    与文案「一致率 < 80% 且 |覆盖率差| > 1 pt」读起来是同一次序，不必来回对。
    """
    out: list[str] = []
    for cl in rule_clauses(rule):
        if cl["metric"] not in out:
            out.append(cl["metric"])
    return out


def series_metric(s: dict, key: str) -> list:
    """从一条类别序列里取某个告警指标的那条线，按指标定义做变换（见 _transform）。"""
    return [_transform(key, v) for v in (s.get(ALERT_METRICS[key].get("src")) or [])]


def rule_vals(rule: dict, s: dict) -> dict[str, list]:
    """
    这条规则要的每条判据 -> 该类的序列，键是**占位符名**（clause_key）。

    判据引擎按占位符取数（_day_result 里就是 vals[clause_key(cl)]），
    于是「同指标、不同阈值」的两条判据各拿各的列名，不会撞在一起。
    """
    return {clause_key(cl): series_metric(s, cl["metric"]) for cl in rule_clauses(rule)}


def global_row(m: dict) -> dict:
    """
    全局域规则的「行」。用 metrics 里的全局量拼成与类别行同样的形状，
    让判据引擎不必知道自己判的是哪一域。

    键 = 每个带 global 的指标行指向的那个 metrics 键，值取它的 value。逐行铺
    而不是写死固定几个：写死的话，新加一条全局指标要记得回来改这里，漏了就是
    一条永远取不到值的规则（RULE_003 的旧写法就是这么哑掉的）。

    同时铺 src 键：metric_value 优先读 src，而 overall_* 这类整体指标两个来源
    都有；只铺 global 的话，全局快照规则会取到 None，**不触发**与「正常」长得
    一模一样。两个键指向同一个 value，不会分叉。
    """
    out = {}
    for d in ALERT_METRICS.values():
        if not d.get("global"):
            continue
        v = (m.get(d["global"]) or {}).get("value")
        out[d["global"]] = v
        if d.get("src"):
            out[d["src"]] = v
    return out


def clause_detail(rule: dict, values: dict[str, float | None]) -> dict:
    """
    「本期实际数」那一句，成对返回。逐**指标**摆值（不是逐判据：同指标的两条判据只说
    一次），不写判词——判词在 condition 里已经有了，这里只说测到了多少。
    判不出来的指标印「数据不足」，不印 0。
    """
    parts = []
    for key in metric_keys_of(rule):
        v = metric_text(key, values.get(key))
        s = ALERT_METRICS[key]["short"]
        parts.append(bi(f"{s['zh']} {v['zh']}", f"{s['en']} {v['en']}"))
    return joina(parts, bi(" · ", " · "))


def row_values(rule: dict, row: dict | None) -> dict[str, float | None]:
    """一行数据上这条规则要的每个指标的值（当期快照规则用）。"""
    return {key: metric_value(row, key) for key in metric_keys_of(rule)}


def series_values(rule: dict, s: dict, k: int) -> dict[str, float | None]:
    """序列第 k 天上这条规则要的每个指标的值（按日规则用）。"""
    return {key: series_metric(s, key)[k] for key in metric_keys_of(rule)}


def _rows_for(rule: dict, row: dict | None) -> dict[str, list]:
    """
    把一行数据铺成「一条序列」，键与判据的占位符同名（见 rule_vals）。
    快照规则只判当天，序列长度就是 1。
    """
    return {clause_key(cl): [metric_value(row, cl["metric"])] for cl in rule_clauses(rule)}


def money(n: float) -> str:
    """与页面上的 money() 同一套写法，别让同一条证据在卡片和别处长得不一样。"""
    a = abs(n)
    if a >= 1e6:
        return f"${n / 1e6:.2f}M"
    if a >= 1e3:
        return f"${n / 1e3:.1f}K"
    return f"${n:.2f}"


def _evidence_of(rule: dict, c: dict | None, m: dict) -> list[str]:
    """
    证据行：只摆数，不解释。分两套是因为类别域和全局域的「数」根本不是同一组——
    硬凑成一套的话，全局待办得显示一堆它并不拥有的类别字段，而类别待办又答不出
    「这个数占全体多少」。

    全局域按**规则自己的指标**摆数（判据指标 + context 里要求对照的）。原来写死
    未分类率那一组：全局规则一多，别的规则的证据行会全都印成「双方均无分类 …」，
    不是本规则的数，读的人还以为是。
    """
    if c is None:
        out = []
        for key in dict.fromkeys(metric_keys_of(rule) + list(rule.get("context") or [])):
            d = ALERT_METRICS[key]
            g = m.get(d.get("global")) or {}
            if g.get("num") is not None:
                lab = d.get("num_label") or bi("分子", "numerator")
                out.append(bi(f"{lab['zh']} {int(g['num']):,} 笔",
                              f"{lab['en']} {int(g['num']):,} rows"))
            if g.get("den"):
                lab = d.get("den_label") or bi("分母", "denominator")
                out.append(bi(f"{lab['zh']} {int(g['den']):,} 笔",
                              f"{lab['en']} {int(g['den']):,} rows"))
            if g.get("value") is not None:
                v = metric_text(key, _transform(key, g["value"]))
                out.append(bi(f"{d['short']['zh']} {v['zh']}", f"{d['short']['en']} {v['en']}"))
        return out
    return [bi(f"差异 {c['diff_count']:,} 笔", f"{c['diff_count']:,} differences"),
            bi(f"差异金额 {money(c['diff_amount'])}", f"{money(c['diff_amount'])} in differences"),
            bi(f"涉及 {c['diff_users']:,} 人", f"{c['diff_users']:,} customers involved"),
            bi(f"可回收 {c['recover_pt']:.2f} pt", f"{c['recover_pt']:.2f} pt recoverable")]


def rule_title(rule: dict) -> dict:
    """规则名成对。缺 en 在加载配置时就中止了，这里直接取，不必兜底。"""
    return bi(rule["title"], rule["en"]["title"])


def rule_hint(rule: dict) -> dict:
    """排查建议成对。没写建议的规则给一对空串——页面上的 esc(null) 与 esc('')
    长得一样，但成对的形状要一致，tx() 才好统一处理。"""
    return bi(rule.get("actionHint") or "", rule["en"].get("actionHint") or "")


def _todo_item(rule: dict, c: dict | None, detail: dict, m: dict) -> dict:
    """按规则造一条待办。全局规则没有类别，c 传 None。

    detail 是 clause_detail 出来的那一对「本期实际数」。文案成对、数字只有一份，
    拼句也是 L 侧一次拼对（分隔符两种语言不同，见 joina）。
    """
    cond = cond_pair(rule)
    it = {
        # name 沿用「类别名」这个字段名（历史库与页面都按它取数），全局项填「全体类别」。
        # 它是**键**不是文案：页面按它把待办分到各个类别块底下，翻译了就对不上号。
        "name": c["name"] if c is not None else GLOBAL_CATEGORY,
        "scope": rule["scope"],
        # 全局命中挂在哪张 ① 指标卡上（类别规则没有这个键）。页面按它把角标
        # 贴到对应卡上；配置加载时已经校验过取值。
        "tile": rule.get("tile"),
        "group": c["group"] if c is not None else None,
        "ruleId": rule["id"],
        "ruleTitle": rule_title(rule),
        # kind 区分两段规则：rule = 当期快照，alert = 按日预警（回填成 alert，
        # 见 _alert_item）。页面据此决定要不要画「连续 N 期」那一块。
        "kind": "rule",
        "condition": cond,
        # Trigger = 判据 + 本期实际数。判据从规则表来（与阈值同源），实际数从数据来，
        # 两段拼起来读的人才不用回页脚对阈值。两段与分隔符都成对，见 joina。
        "trigger": joina([cond, detail], bi("　本期：", " — this run: ")),
        "issueType": rule["issueType"],
        "recommendedAction": rule["recommendedAction"],
        # 排查建议按规则给：同一类问题怎么查是规则的一部分，不该由页面按类别名猜。
        # 运维事项卡片上的「建议」就是它，不同规则给出不同方向。
        "actionHint": rule_hint(rule),
        "ruleMetrics": list(rule["metrics"]),
        "evidence": _evidence_of(rule, c, m),
    }
    if c is not None:
        it.update({
            "type_mix": c["type_mix"],
            "diff_count": c["diff_count"],
            "diff_amount": c["diff_amount"],
            "diff_users": c["diff_users"],
            "recover_pt": c["recover_pt"],
            "diff_rate": c["diff_rate"],
            "illion_count": c["illion_count"],
            "finv_count": c["finv_count"],
            "illion_coverage": c["illion_coverage"],
            "finv_coverage": c["finv_coverage"],
            "coverage_gap": c["coverage_gap"],
            "iou_rate": c["iou_rate"],
            "union_count": c["union_count"],
            "agree_count": c["agree_count"],
            "agree_rate": c["agree_rate"],
        })
    return it


def evaluate_rules(cats: list[dict], metrics: dict) -> list[dict]:
    """
    跑一遍当期快照规则，产出这一期的运维待办（唯一键 = 类别 + 规则 ID）。

    判据是声明式的（配置里写「哪个指标、怎么比、比多少」），这里只管按 scope 找行。
    加一条规则改配置即可，不必再动这个函数——原来的写法是每条规则配一个
    _hit_rule_00N，规则表与判据表变成两份，对不上就是一条永远不触发的规则。

    类别域规则逐类别跑，全局域规则只跑一次。suppressedBy 不在这里判：
    它要跨「当期快照 / 按日预警」两段规则生效，所以统一由 apply_suppression 在合并后判。
    """
    items: list[dict] = []
    for rule in MONITORING_RULES:
        if not rule["enabled"]:
            continue                      # 停用的规则不出待办；页脚规则表仍会列出它
        if rule["scope"] == "global":
            row = global_row(metrics)
            if _day_result(rule, _rows_for(rule, row), 0) is True:
                items.append(_todo_item(rule, None,
                                        clause_detail(rule, row_values(rule, row)), metrics))
            continue
        for c in cats:
            if _day_result(rule, _rows_for(rule, c), 0) is True:
                items.append(_todo_item(rule, c, clause_detail(rule, row_values(rule, c)), metrics))
    return items


def apply_suppression(items: list[dict]) -> list[dict]:
    """
    按 suppressedBy 去重：命中过那几条规则的类别，本条不再报——同一个问题说两遍，
    卡片多一张、状态列多一个要维护，读的人还以为是两件事。

    放在两段规则的产物合并之后跑，只有一个判据：各自 evaluate_* 里各判一次的话，
    「谁先谁后」就由调用顺序暗中决定，改一次调用次序抑制关系就反过来。
    抑制是单向的，按规则表顺序处理——先报的不会被后来者压掉。
    """
    hit: dict[str, set[str]] = {}
    out: list[dict] = []
    for it in items:
        rule = RULE_BY_ID.get(it["ruleId"]) or {}
        if any(it["name"] in hit.get(s, ()) for s in rule.get("suppressedBy", [])):
            continue
        hit.setdefault(it["ruleId"], set()).add(it["name"])
        out.append(it)
    # 同级里全局项排最前（它管的是整体，不该被某个类别的笔数压下去），其余按差异笔数降序
    out.sort(key=lambda x: (x["scope"] != "global", -(x.get("diff_count") or 0)))
    return out


# ── 按日预警（Alerting）───────────────────────────────────────────────────
# 序列一次铺好，判据、卡片、页面上的小图都从这一份取。列名与逐日类别行同名同口径
# （比率就是比率、笔数就是笔数），只有 coverage_gap / diff_rate 是现算的。
ALERT_SERIES_COLS = ("iou_rate", "illion_coverage", "finv_coverage",
                     "diff_count", "diff_amount", "union_count",
                     # 派生列：与 build_categories 同一个式子，不另立口径
                     "coverage_gap", "diff_rate", "miss_rate", "extra_rate")
# 预警卡片上带多少期历史。「这几天在往上爬」看得出趋势即可，再多会把载荷撑起来
ALERT_HISTORY_DAYS = 14


def _cell(row, key: str) -> float | None:
    """
    从一行里取一个数。NaN / 空串 / 非数一律 None——序列里 None 与 0 是两件事，
    0 表示「测到了，是零」，None 表示「这一期没有这个数」。
    """
    v = row.get(key) if isinstance(row, dict) else getattr(row, key, None)
    # 空值有三种长相，都要在这一步挡住：None、NaN（float 列）、NA（可空整数列，
    # 类别历史里新加的两列就是）。pd.NA 不能拿 `== ""` 去比——那返回的还是 NA，
    # 落进 if 是「真假不明」的报错，而不是一个可读的失败。
    if v is None or (not isinstance(v, str) and pd.isna(v)) or v == "":
        return None
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _put_series_row(s: dict, i: int, row) -> None:
    """把一行（历史的 NamedTuple 或当期类别 dict）放进序列的第 i 天。"""
    ill, fin = _cell(row, "illion_coverage"), _cell(row, "finv_coverage")
    dc, un = _cell(row, "diff_count"), _cell(row, "union_count")
    dr = _cell(row, "diff_rate")
    s["illion_coverage"][i] = ill
    s["finv_coverage"][i] = fin
    s["coverage_gap"][i] = None if (ill is None or fin is None) else fin - ill
    s["iou_rate"][i] = _cell(row, "iou_rate")
    s["diff_count"][i] = dc
    s["diff_amount"][i] = _cell(row, "diff_amount")
    s["union_count"][i] = un
    # 行里没有 diff_rate 时，用已有的两个数现算。算不出来就留 None：那几天就是
    # 判不了，不拿别的数顶上。
    s["diff_rate"][i] = dr if dr is not None else ((dc / un) if (dc is not None and un) else None)
    # 漏识别 / 多识别率同路：现算优先，历史行里没有这一列（或那一期还没跑过新列）
    # 就回退到笔数 ÷ 本类并集——与 build_categories 同一个式子。只加列名、不在这里
    # 落值的话，序列里永远是 None，图上那条线从头断到尾。
    mc, ec = _cell(row, "miss_count"), _cell(row, "extra_count")
    mr, er = _cell(row, "miss_rate"), _cell(row, "extra_rate")
    s["miss_rate"][i] = mr if mr is not None else ((mc / un) if (mc is not None and un) else None)
    s["extra_rate"][i] = er if er is not None else ((ec / un) if (ec is not None and un) else None)


# 整体序列用的列。名字必须与趋势表的列同名——series_metric 按
# ALERT_METRICS 的 src 取数，两处同名才能让同一套判据引擎判两个域。
GLOBAL_SERIES_COLS = ("finv_coverage", "illion_coverage", "coverage_adjusted_agreement")


def build_global_series(trend: list[dict], dates: list[str]) -> dict:
    """
    整体指标按天铺成一条序列，日期轴与分类别序列共用。

    只铺判据要用的原始列，不在这里算派生率：派生口径归 build_categories /
    trend_extra，两处各算一遍迟早会分叉。
    """
    by_date = {str(r.get("par_dt")): r for r in trend}
    out = {col: [None] * len(dates) for col in GLOBAL_SERIES_COLS}
    for i, d in enumerate(dates):
        row = by_date.get(d)
        if row is None:
            continue          # 趋势里没有这一天：判不了，留 None，不拿相邻期的数顶上
        for col in GLOBAL_SERIES_COLS:
            out[col][i] = _cell(row, col)
    return out


def build_cat_series(cat_hist_df: pd.DataFrame, cats: list[dict], data_date: str,
                     trend_hist: list[dict] | None = None) -> dict:
    """
    把「每类每天一行」的历史铺成告警引擎要的序列：日期轴 + 每类每列一条同长的值列表。

    在 Python 里一次铺平、随载荷发给页面，而不是让页面按 36 个类别各读一遍历史再各自
    聚合：日期区间一改就要重算一遍，正是「不要在每个类别上重复做聚合」那条。

    日期轴 = 历史里的日期 ∪ 监控日，升序。轴上的日期就是「跑过的期」，中间跳过的日子
    不在轴上——没跑过的日子本来就没有值，摆一个空位出来只会被当成「那天缺数」。

    全局域规则（scope=global）与类别规则共用这条轴，trend_hist 在时一并铺一份
    global；没有传就不铺，evaluate_alerts 会把全局规则判成「数据不足」而不是
    悄悄拿空序列当正常。
    """
    names = [c["name"] for c in cats]
    dates = sorted({str(d) for d in cat_hist_df["par_dt"].astype(str)} | {data_date})
    idx = {d: i for i, d in enumerate(dates)}
    n = len(dates)
    # 先铺满 None：某天没有这个类别（新加的类别）时留 None，不是 0
    series = {name: {col: [None] * n for col in ALERT_SERIES_COLS} for name in names}
    for r in cat_hist_df.itertuples(index=False):
        s = series.get(str(getattr(r, "name")))
        if s is None:
            continue            # 历史里有、本期已经没有的类别：不进序列（没有当期行可判）
        i = idx.get(str(getattr(r, "par_dt")))
        if i is not None:
            _put_series_row(s, i, r)
    # 当期这一份用现算的 cats 覆盖：历史是上一轮跑出来的，本期必须以本次的结果为准
    for c in cats:
        _put_series_row(series[c["name"]], idx[data_date], c)
    out = {"dates": dates, "series": series}
    if trend_hist is not None:
        out["global"] = build_global_series(trend_hist, dates)
    return out


def _insufficient_reason(rule: dict, vals: dict, end: int) -> dict:
    """
    判不出来的原因，逐条摆，成对返回。不写一句笼统的「数据不足」——基准还差两期与
    本期取不到值是两件事，前者等两天就有了，后者要去查数据管道。
    """
    parts: list[dict] = []
    for cl in rule_clauses(rule):
        k = clause_key(cl)
        if _clause_hit(cl, vals[k], end) is not None:
            continue
        lab = ALERT_METRICS[cl["metric"]]["short"]
        if str(cl["op"]).endswith("baseline"):
            got = sum(1 for v in vals[k][max(0, end - cl["window"]):end] if v is not None)
            miss, need = cl["min_days"] - got, cl["min_days"]
            parts.append(bi(f"{lab['zh']} 基准还差 {miss} 期（需 {need} 期、已有 {got} 期）",
                            f"{lab['en']}: baseline is {miss} period(s) short "
                            f"(needs {need}, has {got})"))
        else:
            parts.append(bi(f"{lab['zh']} 本期取不到值",
                            f"{lab['en']}: no value for this period"))
    return joina(parts, bi("；", "; ")) if parts else bi("判据不可用", "the criterion cannot be evaluated")


def _alert_item(rule: dict, c: dict, s: dict, dates: list, end: int,
                days: int, m: dict) -> dict:
    """
    一条触发的预警。前六成字段与快照规则的待办同源（走 _todo_item），另加连续天数
    与判据本身的历史——「为什么判它异常」得能当场看见，而不是让人回去翻底表。
    """
    it = _todo_item(rule, c, clause_detail(rule, series_values(rule, s, end)), m)
    lo = max(0, end - ALERT_HISTORY_DAYS + 1)
    # 判据指标 + 规则要求一并显示的指标（覆盖率差要同时看到两侧的覆盖率）。
    # 两处都按指标名去重，页面拿 key 去 DATA.catSeries 里找不到第二份口径。
    keys = list(dict.fromkeys(metric_keys_of(rule) + list(rule["context"])))
    it["kind"] = "alert"
    it["window"] = rule["window"]
    it["consecutiveDays"] = days
    # 连续期内**实际跑过的日期**。跨了几天按日期能看出来：中间断过的期数不在轴上，
    # 所以「连续 3 期」对应的日期不一定挨着，摊开日期是唯一的交代方式。
    it["streakDates"] = [dates[i] for i in range(end - days + 1, end + 1)]
    # 「连续 N 天」而不是「N 期」：这条看板一天跑一次，期与天在这里是同一个东西，
    # 而规则配置里的文案写的就是「天」。两处用同一个词，读的人才不用换算。
    title, cond = rule_title(rule), cond_pair(rule)
    since = dates[end - days + 1]
    it["reason"] = (bi(f"{title['zh']}：{cond['zh']}，自 {since} 起连续 {days} 天成立",
                       f"{title['en']}: {cond['en']} — holds for {days} consecutive days since {since}")
                    if days > 1 else
                    bi(f"{title['zh']}：{cond['zh']}，本期成立",
                       f"{title['en']}: {cond['en']} — holds this period"))
    it["history"] = {
        "dates": dates[lo:end + 1],
        "values": {k: series_metric(s, k)[lo:end + 1] for k in keys},
        "labels": {k: ALERT_METRICS[k]["short"] for k in keys},
        # 单位随值走，不随名字走：页面按单位决定「乘 100 写百分号 / 写成 pt /
        # 写成笔数 / 写成金额」，而单位定义只在 ALERT_METRICS 一处。页面自己按
        # 指标名猜单位，配置里加一条新判据就会出现一条格式不对的曲线。
        "units": {k: ALERT_METRICS[k]["unit"] for k in keys},
    }
    # 判据之外的对照值（覆盖率差要同时看到两侧的覆盖率），当期值一并带上
    it["context"] = [{"key": k, "label": ALERT_METRICS[k]["short"],
                      "unit": ALERT_METRICS[k]["unit"],
                      "value": metric_text(k, series_metric(s, k)[end])}
                     for k in rule["context"]]
    return it


def evaluate_alerts(series: dict, cats: list[dict], metrics: dict,
                    end: int | None = None) -> dict:
    """
    跑一遍按日规则，产出预警。

    规则有两个域：category 逐类别判，global 只判一次（name = GLOBAL_CATEGORY，
    c = None）。两个域共用同一套判据引擎，区别只在 loop 的目标和取哪条序列；
    全局序列由 build_cat_series 一并铺在 series["global"] 里。

    每个（类别或整体 × 规则）落在四种状态之一，四种都要说得出为什么：

      triggered    今天成立，且连续期数 ≥ window —— 当期预警，进运维事项
      watching     今天成立，但连续期数还不够 —— 不报，另列一块「接近触发」
      recovered    上一期还在报、今天不成立了 —— 按恢复规则退出
      insufficient 今天取不到值、或基准没攒够 —— 明说判不了

    四种都不能省。只分「报 / 不报」的话，判不出来的会被混进「不报」，
    页面上「一切正常」与「我还不知道」长得一模一样。

    恢复规则：**上一期成立过且已达 window、本期不成立** 即为恢复。要求「上一期已达
    window」是因为没到 window 的从来没报出去过，把它的回落叫「恢复」等于凭空发一条
    「有个预警好了」的消息。

    end 是「判到哪一天为止」在日期轴上的下标，默认最后一天（当期）。序列是全历史
    一份，不复制。
    """
    dates: list[str] = series["dates"]
    if end is None:
        end = len(dates) - 1
    # 只保留到这一天为止的日期轴。各列不跟着截——它们按下标取值，下标没变，
    # 「到此为止」这件事由 end 一个数说了算。
    dates = dates[:end + 1]
    rule_infos = []
    for rule in ALERT_RULES:
        rule_infos.append({
            "id": rule["id"], "title": rule_title(rule), "enabled": rule["enabled"],
            "scope": rule["scope"], "tile": rule.get("tile"),
            "window": rule["window"], "condition": cond_pair(rule),
            "issueType": rule["issueType"],
            "recommendedAction": rule["recommendedAction"],
            "actionHint": rule_hint(rule),
            "thresholds": dict(rule["thresholds"]),
            "metrics": list(rule["metrics"]),
            "needHistory": rule_history_need(rule),
        })
    out = {"dates": dates, "rules": rule_infos,
           "rulesConfig": RULES_CONFIG_FILE,
           "items": [], "watching": [], "recovered": [], "insufficient": []}
    if end < 0:
        return out

    by_name = {c["name"]: c for c in cats}
    global_series = series.get("global")
    insufficient: dict[tuple[str, str], dict] = {}
    for rule in ALERT_RULES:
        if not rule["enabled"]:
            continue
        # 类别规则逐类跑；全局规则只跑一次，目标序列是 series["global"]，
        # 报出来的名字统一是 GLOBAL_CATEGORY（与待办身份的键同一个）。
        if rule["scope"] == "global":
            targets = [(GLOBAL_CATEGORY, global_series, None)]
        else:
            targets = [(name, s, by_name[name]) for name, s in series["series"].items()]
        for name, s, c in targets:
            # 序列没铺出来（调用方没给 trend_hist）时明说判不了，不拿空序列当正常。
            # 这里不能走 _insufficient_reason：它要按下标取 vals，空序列会先 IndexError。
            if s is None:
                reason = bi("整体序列不可用", "overall series unavailable")
                rec = insufficient.setdefault(
                    (rule["id"], reason["zh"]),
                    {"ruleId": rule["id"], "ruleTitle": rule_title(rule),
                     "tile": rule.get("tile"), "reason": reason, "names": []})
                rec["names"].append(name)
                continue
            vals = rule_vals(rule, s)
            st = rule_streak(rule, vals, end)
            if st["today"] is None:
                # 同一条规则下原因相同的类别合成一条，只记名字——36 类各写一条的话，
                # 「还差两期」这句话会在页面上出现 36 次。
                # 合并的键用中文那一半：两种语言是同一次调用一起算出来的，中文相同
                # 就必然整对相同，而 dict 当不了键。
                reason = _insufficient_reason(rule, vals, end)
                rec = insufficient.setdefault(
                    (rule["id"], reason["zh"]),
                    {"ruleId": rule["id"], "ruleTitle": rule_title(rule),
                     "tile": rule.get("tile"), "reason": reason, "names": []})
                rec["names"].append(name)
                continue
            if st["today"] is False:
                prev = (rule_streak(rule, vals, end - 1) if end >= 1
                        else {"today": None, "days": 0})
                if prev["today"] is True and prev["days"] >= rule["window"]:
                    out["recovered"].append({
                        "ruleId": rule["id"], "ruleTitle": rule_title(rule), "name": name,
                        "tile": rule.get("tile"),
                        "lastDate": dates[end - 1], "hadDays": prev["days"],
                        "current": clause_detail(rule, series_values(rule, s, end)),
                    })
                continue
            if st["days"] >= rule["window"]:
                out["items"].append(_alert_item(rule, c, s,
                                                dates, end, st["days"], metrics))
            else:
                out["watching"].append({
                    "ruleId": rule["id"], "ruleTitle": rule_title(rule), "name": name,
                    "tile": rule.get("tile"),
                    "days": st["days"], "window": rule["window"],
                    "need": rule["window"] - st["days"],
                    "current": clause_detail(rule, series_values(rule, s, end)),
                    "condition": cond_pair(rule),
                })
    out["insufficient"] = sorted(insufficient.values(),
                                 key=lambda r: (r["ruleId"], -len(r["names"])))
    return out


# ── 变化检测（What Changed）───────────────────────────────────────────────
def find_trend_row(trend: list[dict], data_date: str) -> dict | None:
    """按 par_dt 精确取某一期。不用 rows[-1]：输入没更新时最后一行就是本期自己。"""
    for r in trend:
        if str(r.get("par_dt")) == str(data_date):
            return r
    return None


def prev_trend_row(trend: list[dict], data_date: str) -> dict | None:
    """data_date 之前最近的那一期。没有更早的期就返回 None（首期没有「变化」可言）。"""
    prior = [r for r in trend if str(r.get("par_dt") or "") < str(data_date)]
    return max(prior, key=lambda r: str(r["par_dt"])) if prior else None


def change_direction(d: float | None) -> str | None:
    """
    方向只有三档，且**不含程度词**：变了多少看 delta，要不要紧由读的人对着规则判。
    「略微下降」「显著下降」这类词把两件事揉进一个字段，正是要避免的。
    """
    if d is None:
        return None
    if abs(d) * 100 < CHANGE_STABLE_PP:
        return "Stable"
    return "Increase" if d > 0 else "Decrease"


def build_metrics(summary: dict, cur: dict | None) -> dict:
    """
    ① 顶部指标卡的值。数值一律取趋势本期的行（与图表同一份数），
    分子 / 分母取 summary——页面上写「N 笔 ÷ M 笔」时不用自己再算一遍。

    global_agreement 不进 METRIC_DEFS：它没有趋势列（趋势里的一致率是并集口径），
    混进那张表会撞上「每一行都要回到趋势本期行」的自检。它两个零件都在 summary 里，
    在这里合一次，页面只渲染不算。
    """
    cur = cur or {}
    total = summary.get("total")
    union = cur.get("union_rows")
    out = {
        "illion_coverage": {"value": cur.get("illion_coverage"),
                            "num": summary.get("illion_coverage_num"),
                            "den": summary.get("illion_coverage_den")},
        "bscat_coverage": {"value": cur.get("finv_coverage"),
                           "num": summary.get("finv_coverage_num"),
                           "den": summary.get("finv_coverage_den")},
        "agreement": {"value": cur.get("coverage_adjusted_agreement"),
                      "num": summary.get("coverage_adjusted_agreement_num"),
                      "den": summary.get("coverage_adjusted_agreement_den")},
        "difference": {"value": cur.get("all_difference_rate"),
                       "num": summary.get("all_difference_count"),
                       # 分母是「并集」不是「全量样本」：值本身 = 差异笔数 ÷ 并集，
                       # 这里原来写 total，卡上印的 57,372 ÷ 269,790 与那个百分数对不上
                       "den": summary.get("all_difference_count_den")},
        "miss": {"value": cur.get("miss_rate"),
                 "num": summary.get("illion_only"), "den": union},
        "extra": {"value": cur.get("extra_rate"),
                  "num": summary.get("finv_only"), "den": union},
        "unknown": {"value": cur.get("unknown_rate"),
                    "num": cur.get("both_empty_rows"), "den": total},
    }
    # 全局一致率 = 标签相同的样本数 ÷ 全量样本数。分母是「全量样本」（含双方都没分类
    # 的样本），与 agreement（分母 = 双方至少一侧有分类的并集）差着一块未分类样本，
    # 两个数都留着：前者回答「在所有交易里有多少猜对了」，后者回答「在模型开口的
    # 那些里有多少猜对了」。别拿 iou_weighted 替代——那个的分母也是全量并集。
    ga_num = summary.get("coverage_adjusted_agreement_num")
    out["global_agreement"] = {
        "value": (ga_num / total) if (ga_num is not None and total) else None,
        "num": ga_num,
        "den": total,
    }
    return out


def build_changes(trend: list[dict], data_date: str) -> list[dict]:
    """
    What Changed：七行指标的「本期 vs 上一监控日」。方向与「是不是真的走坏了」
    （worsened）都在这里算完，页面只渲染不重算——判据两边各写一份，改了一边就会不一致。

    上一期按 par_dt 显式查找，不取 rows[-1]：输入没更新时最后一行就是本期自己，
    拿自己和自己比会得出「全部持平」这种最糟的假结论。

    措辞中立：这里只说「较上一监控日上升 / 下降 N pt」，不写「模型变好 / 变差」——
    变化是读数，好坏是判断，判断得由人看着证据下。
    """
    cur = find_trend_row(trend, data_date)
    prev = prev_trend_row(trend, data_date)
    if cur is None or prev is None:
        return []              # 只有一期：没有可比的对象，页面走「变化数据不可用」

    out = []
    for key, label, col, dirn in METRIC_DEFS:
        c, p = cur.get(col), prev.get(col)
        if c is None or p is None or pd.isna(c) or pd.isna(p):
            # 旧格式的历史行没有这一列（如 unknown_rate 是后加的）。留空并标明不可用，
            # 绝不用 0 顶替——0 会被读成「测到了，是零」。
            out.append({"key": key, "label": label, "current": None, "previous": None,
                        "delta": None, "direction": None,
                        "dir": dirn, "worsened": False})
            continue
        d = float(c) - float(p)
        bad = (dirn == "up_good" and d < 0) or (dirn == "down_good" and d > 0)
        out.append({"key": key, "label": label, "current": float(c), "previous": float(p),
                    "delta": d, "direction": change_direction(d),
                    "dir": dirn,
                    # 「这一个指标这期是不是真的往坏方向走满了一个阈值」。页面上那句
                    # 「走坏」就按它筛——不按方向筛，是因为方向把「涨了一点」与
                    # 「涨过了一个阈值」说成同一件事。
                    "worsened": bool(bad and abs(d) * 100 >= CHANGE_BAD_PP)})
    return out


# ── 待办状态：每次运行现算，不跨运行保存 ─────────────────────────────────
def default_todo_status(items_by_day: dict[str, list[dict]]) -> dict:
    """
    按本次取数件里的日期范围给每个待办建一条状态。

    不再跨运行保存处理进度：状态一律从 New 开始，首次 / 最近触发日期取本次数据里
    最早 / 最晚出现的那天。同一件事在 18 天里反复触发，仍然是同一行。
    """
    index: dict[tuple[str, str], dict] = {}
    for d in sorted(items_by_day):
        for it in items_by_day[d]:
            k = (it["name"], it["ruleId"])
            cur = index.get(k)
            if cur is None:
                index[k] = {"status": ENUM_FALLBACK["taskStatus"],
                            "first_trigger_date": d, "last_trigger_date": d}
            else:
                cur["last_trigger_date"] = max(cur["last_trigger_date"], d)
    return {"map": index}


def attach_todo_status(items: list[dict], status_map: dict, data_date: str) -> None:
    """把状态表里的处理进度贴到本次的待办上。表里没有的按 New（上一步已经建好行）。"""
    for it in items:
        rec = status_map.get((it["name"], it["ruleId"])) or {}
        it["status"] = rec.get("status") or ENUM_FALLBACK["taskStatus"]
        it["firstTriggerDate"] = rec.get("first_trigger_date") or data_date
        # Source Date = 这一项**最近一次**被触发的日期。与首次触发日期不同时，
        # 页面上会把首次另外标出来——否则「今天新报的」和「报了两周还没处理」长得一样。
        it["sourceDate"] = rec.get("last_trigger_date") or data_date


def build_todo(items: list[dict]) -> dict:
    """
    To-Do 汇总。条目本身只按「类别 + 规则 ID」认人，不再分档——P0/P1/P2 现在只
    针对类别（见 LEVEL_BY_SEGMENT），待办这一层没有等级这回事。

    diff_count / diff_amount / recover_pt 三项必须**按去重后的类别**汇总：
    一个类别同时命中两条规则会出现两个条目，逐条相加就把这个类别的差异数了两遍，
    而那个数会直接显示在页面上（「合计 N 笔差异」），错得看不出来。
    """
    by_cat: dict[str, dict] = {}
    for it in items:
        if it["scope"] == "category":
            by_cat.setdefault(it["name"], it)
    return {
        "items": items,
        "rules": rule_thresholds(),
        "global": sum(1 for x in items if x["scope"] == "global"),
        "categories": len(by_cat),
        "diff_count": sum(x["diff_count"] for x in by_cat.values()),
        "diff_amount": sum(x["diff_amount"] for x in by_cat.values()),
        "recover_pt": sum(x["recover_pt"] for x in by_cat.values()),
        "statusCounts": {s: sum(1 for x in items if x["status"] == s)
                         for s in ENUMS["taskStatus"]},
    }


def rules_table() -> list[dict]:
    """
    页脚规则表：从两段规则表渲染，不在页面上另抄一遍阈值和条件。
    页面上看到的判据，就是脚本真正跑的那条。

    enabled 也要送出去：停用的规则仍然列在表里（否则一条判据会不声不响地消失），
    但要标明它已经停了，免得读的人以为它还在跑。
    """
    def row(r: dict, section: str) -> dict:
        return {
            "id": r["id"],
            "section": section,
            "enabled": r["enabled"],
            "scope": r["scope"],
            "tile": r.get("tile"),
            "title": rule_title(r),
            "window": r.get("window", 1),
            "condition": cond_pair(r),
            "thresholds": dict(r["thresholds"]),
            "metrics": list(r["metrics"]),
            "issueType": r["issueType"],
            "recommendedAction": r["recommendedAction"],
            "suppressedBy": list(r.get("suppressedBy", [])),
            "actionHint": rule_hint(r),
        }

    return ([row(r, "snapshot") for r in MONITORING_RULES]
            + [row(r, "alerts") for r in ALERT_RULES])


def build_category_details(detail: pd.DataFrame) -> dict:
    """
    明细按「类别 → 差异类型 → 差异流向」三级组织，供问题诊断逐层点开。
    页面上漏识别 / 多识别两个类型**不渲染流向这一层**（2026-10-13 用户定）：这两个
    类型的一侧分类恒为 (空)，流向标签只是把类别名抄了一遍，点开等于白点一次——
    树照旧带 flows（数据是完整的），合掉那一层是模板侧的事。

    2026-11-13 起不再内嵌逐笔交易示例：每个流向只保留 count / amount / users，
    页面上看到的是聚合结果，不再放某几笔样本。

    类型这一级切在流向之前：同一个流向里可能混着两种差异类型，先把流向合起来、
    再按主导类型归类，类型层的数就和类别层的构成（type_mix）对不上了，
    点开看到的是另一个数。先按类型切再按流向切，逐层相加处处严格相等。

    类别归属与 build_categories 用的是同一套规则（illion 侧优先，illion 为空时归
    finv 侧），所以某个类别下各类型、各流向笔数之和恒等于该类的差异笔数——
    点开看到的明细就是上一层那个数字的构成，对不上说明归因逻辑被改坏了。
    """
    bucket, types = bucket_and_type(detail)
    d = detail.copy()
    d["_cat"], d["_type"] = bucket, types

    keys = [k for k, *_ in MISMATCH_TYPES]
    # 源表措辞变了（出现没映射过的排查类型）时，这一笔不并进任何一个已知类型——
    # 并进去等于悄悄改了构成比。归到 UNLABELED 里由自检喊出来。
    d["_type"] = d["_type"].where(d["_type"].isin(keys), UNLABELED_TYPE)

    groups = d.groupby(["_cat", "_type", "差异流向"], sort=False)
    sizes: dict = groups.size().to_dict()
    # 用户数必须按类别整体去重，不能把各流向的 nunique 相加——同一用户会跨流向出现
    cat_users = d.groupby("_cat")["user_id"].nunique().to_dict()

    out: dict[str, dict] = {}
    for (cat, typ, flow), n in sorted(sizes.items(), key=lambda kv: -kv[1]):
        g = groups.get_group((cat, typ, flow))
        node = out.setdefault(cat, {
            "count": 0, "amount": 0.0, "types": [], "unlabeled": 0,
            "users": int(cat_users.get(cat, 0)),
            "type_mix": {k: 0 for k, *_ in MISMATCH_TYPES},
        })
        # 类型构成按整组统计
        node["count"] += n
        node["amount"] = round(node["amount"] + float(g["amount"].abs().sum()), 2)
        if typ == UNLABELED_TYPE:
            node["unlabeled"] += n
        else:
            node["type_mix"][typ] += n
            tnode = next((t for t in node["types"] if t["key"] == typ), None)
            if tnode is None:
                tnode = {"key": typ, "count": 0, "flows": []}
                node["types"].append(tnode)
            tnode["count"] += n
            frm, _, to = flow.partition(" → ")
            tnode["flows"].append({
                "from": show(frm),
                "to": show(to),
                "count": n,
                "amount": round(float(g["amount"].abs().sum()), 2),
                "users": int(g["user_id"].nunique()),
            })

    for node in out.values():
        for t in node["types"]:
            t["flows"].sort(key=lambda f: -f["count"])
        # 类型顺序 = MISMATCH_TYPES 的固定顺序（红→紫→黄），不按笔数排——
        # 色盲配对的前提就是这个顺序（见 MISMATCH_TYPES 的注释），动了等于换配色方案
        node["types"].sort(key=lambda t: keys.index(t["key"]))
    return out


def bucket_and_type(detail: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    每笔差异的归属 Category 和类型 key。

    归属按 illion 侧：illion 有分类时算给该类，illion 为空时算给 finv 侧。
    类型优先取「排查类型」，取不到时回落到「Category比对状态」——
    两张表用词不同，但都映射回同一组 key。
    """
    ill = detail["illion_Category"]
    ill_empty = ill.apply(is_empty)
    bucket = ill.where(~ill_empty, detail["finv_Category"]).apply(show)
    types = detail["排查类型"].map(TYPE_BY_LABEL)
    types = types.where(types.notna(), detail["Category比对状态"].map(TYPE_BY_LABEL))
    return bucket, types


def build_categories(cat: pd.DataFrame, detail: pd.DataFrame, sample_total: int,
                     union: int) -> list[dict]:
    """
    差异笔数按 illion 侧归属：illion 有分类时算给该 Category，
    illion 为空时算给 finv 侧——归属后各行之和 == Category 差异总数。

    覆盖率与一致率口径：
      illion/finv 覆盖率 = 该侧判为该类的交易数 / 全量样本数
      类别一致率        = 一致数量 / illion数量   （illion 判为该类的交易中 finv 也判为该类的比例）
      一致率（交集÷并集）= 一致数量 / 该类并集
      分类别一致率合计  = 一致数量 / 全量并集     （各类相加 = 整体一致率，即 ① 的「一致率」）
      全局一致率        = 一致数量 / 全量样本     （分母含双方都没分类的样本，比上面那个小）
      输出分布占比      = 该类的 finv数量 / bscat 已分类总数（各类相加 = 100%，用于看分布）

    金额口径：差异金额取 |amount| 之和，归因规则与差异笔数一致（含 finv 回填行），
    因此比源表「差异交易金额」（仅 illion 侧）大。为让占比始终 <= 100%，分母用
    该类涉及的交易总额 = illion 该类金额 + finv 独有金额（源表 illion金额 不含后者）。

    recover_pt = 差异笔数 ÷ 并集。这个数可以直接加：全部修掉就是整体提升的点数，
    因为 差异总数 ÷ 并集 == 1 − 交集÷并集，每一笔差异恰好等于一个单位的可回收一致率。
    """
    ill = detail["illion_Category"]
    ill_empty = ill.apply(is_empty)
    bucket, types = bucket_and_type(detail)
    counts = bucket.value_counts().to_dict()
    type_counts = pd.crosstab(bucket, types).to_dict(orient="index")
    abs_amt = detail["amount"].abs()
    amounts = abs_amt.groupby(bucket).sum().to_dict()
    # illion 侧为空的行，其金额不在源表 illion金额 里，需要补进分母
    backfill = abs_amt[ill_empty].groupby(detail["finv_Category"][ill_empty].apply(show)).sum().to_dict()
    diff_total = sum(counts.values())
    # 输出分布的分母 = bscat 判了类的交易数。每笔交易只有一个 finv 类别，所以各类
    # 相加恰好等于这个数（自检里对着 summary 的 finv 覆盖率分子核）。
    finv_total = int(cat["finv数量"].sum())

    rows = []
    for _, r in cat.iterrows():
        name = str(r["Category"]).strip()
        illion_n = int(r["illion数量"])
        finv_n = int(r["finv数量"])
        agree_n = int(r["一致数量"])
        union_n = int(r["并集数量"])
        diff_amt = float(amounts.get(name, 0.0))
        illion_amt = num(r["illion金额"])
        category_amt = illion_amt + float(backfill.get(name, 0.0))
        # illion 侧无该类的样本时为 0/0，无意义，返回 None 由前端显示为「—」
        # 漏识别 / 多识别的笔数先算出来：下面 type_mix 用它，两个率（÷ 本类并集）
        # 也用这一份
        mix = {k: int(type_counts.get(name, {}).get(k, 0)) for k, *_ in MISMATCH_TYPES}
        agree_rate = (agree_n / illion_n) if illion_n else None
        # 一致率（交集÷并集）：与看板主数字同一口径，To-Do 判据用的就是它，
        # 和上面的 agree_rate（÷ illion 数量）不是一回事，别混用
        iou_rate = (agree_n / union_n) if union_n else None
        coverage_gap = (finv_n - illion_n) / sample_total if sample_total else 0.0
        rows.append({
            "name": name,
            "illion_count": illion_n,
            "finv_count": finv_n,
            "agree_count": agree_n,
            "union_count": union_n,
            "illion_coverage": illion_n / sample_total if sample_total else 0.0,
            "finv_coverage": finv_n / sample_total if sample_total else 0.0,
            "coverage_gap": coverage_gap,
            "agree_rate": agree_rate,
            "iou_rate": iou_rate,
            # 差异率 = 该类差异笔数 ÷ **该类自己的并集**。注意与 recover_pt 的分母不同：
            # 那个除的是全量并集，所以可跨类相加（各类之和 = 整体可回收点数）；
            # 这个除的是本类并集，只在类内说得通，把几个类的 diff_rate 加起来没有意义。
            "diff_rate": (counts.get(name, 0) / union_n) if union_n else None,
            # 「该不该动手」不在这里定——那是规则的事（见 MONITORING_RULES 与
            # evaluate_rules）。这个函数只管测量，测出来的数被规则、页面、
            # 历史库各取所需；把判定塞进测量函数里，两处判据迟早分叉。
            # 输出分布占比（第四部分 Share 列）：该类的 finv 笔数 ÷ bscat 已分类总数
            "output_share": (finv_n / finv_total) if finv_total else None,
            # 全局一致率（第三部分）：一致数量 ÷ 全量并集，即该类对整体一致率的贡献。
            # 分子与 iou_rate 相同、分母换成全量并集，所以各类相加 = 200999/258371
            # = 第一部分那个整体一致率。分母不能用全量样本——那样会把双方均无分类
            # 的交易也算进分母，口径就不再是「交集÷并集」了。
            "iou_weighted": (agree_n / union) if union else 0.0,
            "illion_amount": illion_amt,
            "finv_amount": num(r["finv金额"]),
            "category_amount": category_amt,
            "diff_amount": diff_amt,
            "diff_amount_share": (diff_amt / category_amt) if category_amt > 0 else None,
            "diff_users": int(r["差异用户数"]),
            "diff_count": int(counts.get(name, 0)),
            "share": counts.get(name, 0) / diff_total if diff_total else 0.0,
            "group": "",          # 业务板块，由 build_segments 回填
            # 差异构成：这一类里漏识别 / 不一致 / 多识别各多少笔
            "type_mix": mix,
            # 漏识别率 / 多识别率 = 本类漏判 / 多判笔数 ÷ **本类并集**，与类别一行里
            # 的一致率、差异率同一个分母（各类相加没有意义，见 diff_rate 那条注释）。
            # ① 那张卡上的两个率是整体口径（÷ 全量并集），两者不是同一个数，别混用。
            "miss_count": int(mix["miss"]),
            "extra_count": int(mix["extra"]),
            "miss_rate": (mix["miss"] / union_n) if union_n else None,
            "extra_rate": (mix["extra"] / union_n) if union_n else None,
            # 修掉本类全部差异，交集÷并集能回收的点数（可跨类直接相加）
            "recover_pt": (counts.get(name, 0) / union * 100) if union else 0.0,
        })

    rows.sort(key=lambda r: -r["diff_count"])
    acc = 0.0
    for r in rows:
        acc += r["share"]
        r["cum_share"] = acc
    return rows


# ── 渲染 ──────────────────────────────────────────────────────────────────
def sanitize(o):
    """NaN / Inf 不是合法 JSON，且嵌进 JS 后会变成字面量 NaN 显示在页面上，统一转 null。"""
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    if isinstance(o, dict):
        return {k: sanitize(v) for k, v in o.items()}
    if isinstance(o, list):
        return [sanitize(v) for v in o]
    return o


# ── 取值 ──────────────────────────────────────────────────────────────────
# 页面上的数都要先过这里：NaN、空串、读不出来的值一律落成 None（页面显示「—」）。
def _f(v) -> float | None:
    """读不出来、是 NaN、是空串一律 None（页面显示「—」）。"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return n if math.isfinite(n) else None


def day_facts(sm_day: pd.DataFrame, cat_day: pd.DataFrame, gr_day: pd.DataFrame,
              det: pd.DataFrame) -> dict:
    """
    某一天的聚合事实。四份输入都必须是**那一天**的切片（见 rows_on）——取数件是多日
    批量，整表进来会把 18 天加在一起，而这些数恰好都在合理量级上，看不出错。

    只算「这一天是多少」，不含任何判定：规则、档位、预警都在后面按天跑（见 main）。
    """
    summary = build_summary(sm_day)
    sample_total = int(summary.get("total") or 0)
    union = int(summary.get("coverage_adjusted_agreement_den") or 0)
    cats = build_categories(cat_day, det, sample_total, union)
    segments = build_segments(gr_day, cats, sample_total)
    # P 档要等 build_segments 之后：它认的是板块归属（见 cat_level）
    assign_levels(cats)
    return {
        "summary": summary,
        "sample_total": sample_total,
        "union": union,
        "cats": cats,
        "segments": segments,
        "mismatch": build_mismatch(det),
        # 差异金额 = 差异交易 |amount| 之和，与分类别的 diff_amount 同源（illion 侧
        # 优先归因，见 build_categories）。更早的历史行没有这一列，图上断开，
        # 不拿笔数去反推。
        "amount": round(float(det["amount"].abs().sum()), 2),
        "rows": len(det),
    }


def render(payload: dict, template: str) -> str:
    blob = json.dumps(sanitize(payload), ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False)
    # </script> 会提前闭合脚本标签，必须打断
    blob = blob.replace("</", "<\\/")
    return template.replace("/*__PAYLOAD__*/null/*__ENDPAYLOAD__*/", blob)


def main() -> int:
    ap = argparse.ArgumentParser(description="BSCAT 分类模型效果监控")
    ap.add_argument("--skip-sync", action="store_true",
                    help="跳过线上取数，直接用 input/ 现有文件出图")
    ap.add_argument("--strict", action="store_true",
                    help="自检不通过时直接失败退出，适合定时任务")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    in_dir = root / "input"
    out_dir = root / "output"
    tpl_path = root / "src" / "template.html"

    if not tpl_path.exists():
        raise SystemExit(f"缺少模板文件: {tpl_path}")
    out_dir.mkdir(parents=True, exist_ok=True)

    # 规则先装上再读数据：配置写错要在**开跑之前**停住。跑到一半才报的话，
    # 日志里已经有一堆正常的读取信息，很难看出失败的原因是配置而不是数据。
    cfg_path = root / "src" / RULES_CONFIG_FILE
    set_rules(*load_rules_config(cfg_path))
    # 两段都可以是空的：2026-11-13 用户把预警规则也全部移除了，只剩看板本身。
    # 0 条规则是合法的运行结果，页面照常出，只是不产生待办和预警。
    log(f"规则配置: {len(MONITORING_RULES)} 条快照 + {len(ALERT_RULES)} 条按日 -> {cfg_path}")
    if not MONITORING_RULES and not ALERT_RULES:
        log("规则配置为空：本次不生成任何待办或预警")

    # 先同步再读数据：线上 5 张 dash 表落到 input/，本次出图用的就是这一批新数据。
    if args.skip_sync:
        log("跳过线上取数，直接使用 input/ 现有文件")
    else:
        log(f"同步线上数据 -> {in_dir}")
        if sync_all(in_dir) != 0:
            raise SystemExit("数据同步失败，已中止；如要直接用 input/ 现有文件出图，加 --skip-sync")

    log(f"读取 {in_dir}")
    sm = normalize_dates(load(in_dir, "summary"), "summary")
    tr = normalize_dates(load(in_dir, "trend"), "trend")
    gr = normalize_dates(load(in_dir, "group"), "group")
    cat = normalize_dates(load(in_dir, "category"), "category")
    log(f"汇总 {len(sm):,} 行 · 趋势 {len(tr):,} 行 · 板块 {len(gr):,} 行 · 类别 {len(cat):,} 行")

    # 当期 = 批量里**最新**的那一天。取数件按天批量导出、行序不是日期序：趋势表的
    # 最后一行是中间某天（这批是 10-06，当期其实是 10-08），按行取会把它的数字
    # 印成最新一期，而且不报任何错。
    days_all = batch_days(tr)
    data_date = days_all[-1]
    log(f"取数件覆盖 {len(days_all)} 天（{days_all[0]} ~ {data_date}），当期 {data_date}")

    # ── 逐日过一遍明细：算完一天扔一天 ──
    # 明细是四份里唯一大到不能整表进内存的一份（780 万行 / 2.3 GB）。当期那一份
    # **留到最后**：它的明细树要等规则跑完（规则决定哪些类别在待办里，页面上标不标
    # 「判不了/已恢复」）。其余几天只用聚合结果。
    facts: dict[str, dict] = {}
    detail_now: pd.DataFrame | None = None
    for d, det in detail_days(in_dir / FILES["detail"]):
        if d not in set(days_all):
            raise SystemExit(f"明细表里有 {d} 这一天，趋势表里没有——取数件几份对不上")
        facts[d] = day_facts(rows_on(sm, d, "summary"), rows_on(cat, d, "category"),
                             rows_on(gr, d, "group"), det)
        if d == data_date:
            detail_now = det
        else:
            del det
    missing = [d for d in days_all if d not in facts]
    if missing:
        raise SystemExit(f"趋势表里的 {missing} 在明细表里没有对应的行")
    log(f"明细逐日算完 {len(facts)} 天，当期那一份留在内存里（{len(detail_now):,} 行）")

    # 趋势的派生列按天算好再并：summary 也是多天的，拿一天的派生值铺给整批行，
    # 18 天会共用同一条 unknown_rate（见 plan_trend）。
    extras = {}
    for d in days_all:
        e = trend_extra(facts[d]["summary"])
        e["mismatch_amount"] = facts[d]["amount"]
        extras[d] = e
    trend_hist_df, trend_hist = plan_trend(tr, extras)
    # 分类别历史：取数件里的这几天全并进来——判预警、算 Δ 用的序列都从它铺。
    cat_hist_df = pd.DataFrame()
    for d in days_all:
        cat_hist_df, _ = plan_category_history(cat_hist_df, facts[d]["cats"], d)
    # 分类别序列铺一次，全历史都在里面。铺在这里而不是各自的调用点：按天判预警时
    # 每个历史日各铺一遍，90 期 × 36 类 × 8 列就是两百万次搬运，而那份矩阵本来
    # 就只有一份。判「截至哪一天」用一个下标（end）指，不复制。
    cats = facts[data_date]["cats"]
    cat_series = build_cat_series(cat_hist_df, cats, data_date, trend_hist)
    # 待办由两段规则生成（快照 + 按日预警），合并之后统一去重：抑制关系要跨两段
    # 生效（当年是 RULE_002 被 RULE_001 压住；那两条 2026-10-16 已删，现在配置里
    # 没有规则用 suppressedBy——判据与去重都留着），各自判一次的话「谁先谁后」就由
    # 调用顺序暗中决定了。按天判：那天报了什么就是那一天的属性。
    items_by_day: dict[str, list[dict]] = {}
    alert_res: dict | None = None
    for d in days_all:
        f = facts[d]
        # ① 那七张卡的值取趋势**那一期**的行——和两张图用的是同一份数，页面不另算
        f["metrics"] = build_metrics(f["summary"], find_trend_row(trend_hist, d))
        res = evaluate_alerts(cat_series, f["cats"], f["metrics"],
                              end=cat_series["dates"].index(d))
        items_by_day[d] = apply_suppression(evaluate_rules(f["cats"], f["metrics"]) + res["items"])
        if d == data_date:
            alert_res = res
    items = items_by_day[data_date]
    # ① 那七张卡与趋势本期行同源（见循环里那句）：页面顶层用的是当期那一份
    metrics = facts[data_date]["metrics"]
    # 「一条预警都没有」有两种原因：真的都没越线，还是历史不够长判不出来。
    # 后者必然发生在刚上线的头几天，写进日志，省得去翻规则配置猜。
    short_rules = [r["id"] for r in alert_res["rules"]
                   if r["enabled"] and r["needHistory"]["streak"] > len(cat_series["dates"])]
    if short_rules:
        log(f"历史 {len(cat_series['dates'])} 期，规则 {short_rules} 的连续天数要求还没满足，"
            f"这几条本期判为数据不足（页面上会明说）")
    # 待办状态只按本次取数件的日期范围生成：默认 New，首次 / 最近触发日期来自
    # 这几天里它最早 / 最晚出现的那一天。
    todo_status = default_todo_status(items_by_day)
    attach_todo_status(items, todo_status["map"], data_date)
    todo = build_todo(items)
    cat_history_days = int(cat_hist_df["par_dt"].nunique())
    summary = facts[data_date]["summary"]
    sample_total = facts[data_date]["sample_total"]
    segments = facts[data_date]["segments"]
    prev_cats = prev_category_snapshot(cat_hist_df, data_date)
    changes = build_changes(trend_hist, data_date)
    prev_date = (prev_trend_row(trend_hist, data_date) or {}).get("par_dt")
    # 明细由 detail 表现算，不取 flow 表——点开类别要能和类别层的差异笔数对上账，
    # 两条并行的聚合路径迟早会算出两个数。页面上只放聚合后的类型 / 流向，不嵌逐笔示例。
    cat_details = build_category_details(detail_now)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    payload = {
        "generatedAt": generated_at,
        "dataDate": data_date,
        "total": sample_total,
        "diffTotal": int(summary.get("all_difference_count") or 0),
        # 变化检测的两个阈值跟着载荷走：② 的悬停文案要写这两个数，页面里再抄一遍
        # 就会和脚本里的实际判据分家。
        "changeStablePt": CHANGE_STABLE_PP,
        "changeBadPt": CHANGE_BAD_PP,
        # 受控枚举 + 非法值落点。JS 侧 const ENUMS = DATA.enums 直接引用这份，
        # 页面不另抄一份名单——抄两份迟早分叉，分叉的症状正是枚举要防的「同义词并存」。
        "enums": ENUMS,
        "enumFallback": ENUM_FALLBACK,
        # ① 四张卡的值。页面只渲染不重算：口径、分子分母都在这里定死，
        # JS 里再算一遍迟早会和图表那几条线分叉。
        "metrics": metrics,
        # 变化检测的结果。历史不足时是空列表（不是七行 null）——② 的摘要据此
        # 写「还比不了」，不摆一行行「—」冒充测过。
        "changes": changes,
        "prevDate": prev_date,
        # 只出当期这一屏，不做日期回溯：dates 只有当期一天、days 为空。
        # 页面按「选中的日期 == latestDate」走顶层载荷那条路。
        "latestDate": data_date,
        "dates": [data_date],
        "days": {},
        "historyNotice": [],
        # 规则表：页脚的判据来源。页面上的阈值说明也从 DATA.todo.rules 取，
        # 两处同源，改 MONITORING_RULES 一处即生效。
        "rulesTable": rules_table(),
        "summary": summary,
        "trend": trend_hist,
        "mismatchTypes": facts[data_date]["mismatch"],
        "todo": todo,
        # ── 分类别趋势序列 + 按日预警 ──
        # 序列是全历史一份矩阵（日期轴 × 36 类 × 8 列），② 的 36 块小图与 ③ 的预警
        # 判据都从它取；回溯某一天时不复制序列，页面按 dataDate 截断即可。
        "catSeriesCols": list(ALERT_SERIES_COLS),
        "catSeries": {"dates": cat_series["dates"],
                      "series": cat_series["series"]},
        # 预警分四种状态，条目本身在 todo.items 里（kind == "alert"），
        # 这里只带「触发了什么之外」的那几块，不重复放一份条目清单。
        "alerts": {k: v for k, v in alert_res.items() if k != "items"},
        "catDetails": cat_details,
        # 分类别指标：卡头的读数、迷你图、排序都用它。
        # 顺序沿用 build_categories 的结果（按差异笔数降序），只带页面要的字段。
        # level = P0/P1/P2 类别优先级（唯一定义处见 LEVEL_BY_SEGMENT）：卡头徽标、
        # 两处目录的点色、排序档都读它。三档状态与「关键类别」2026-10-13 撤掉；
        # 源表「建议优先级」「排查优先级」2026-11-13 已从上游删除，P 档只有这一套，
        # 「关键Category」仍在源表里，但不读。
        # d_* 是与上一期的差：没有上一期（首期）或那一期该字段为空时一律 None，
        # 页面上显示「—」——拿 0 顶替会被读成「没有变化」。表格只印 Δ 不印上期绝对值。
        "categories": [{
            "name": c["name"],
            "group": c["group"],          # 业务板块，见 build_segments
            "level": c["level"],          # P0/P1/P2，见 cat_level
            # 差异率 = 该类差异笔数 ÷ 该类并集（不可跨类相加，见 build_categories）
            "diff_rate": c["diff_rate"],
            # 本类的漏判 / 多判笔数。② 的「漏判最多 / 多判最多」两个排序档要用；
            # 率（÷ 本类并集）不进载荷——那一列已经逐日躺在 catSeries 里了，
            # 而排序比的是笔数，不是率
            "miss_count": c["miss_count"],
            "extra_count": c["extra_count"],
            "illion_count": c["illion_count"],
            "finv_count": c["finv_count"],
            # 输出分布占比 = 该类 finv 笔数 ÷ bscat 已分类总数，各类相加 = 100%
            "output_share": c["output_share"],
            "illion_coverage": c["illion_coverage"],
            "finv_coverage": c["finv_coverage"],
            "coverage_gap": c["coverage_gap"],
            "iou_rate": c["iou_rate"],
            # 该类一致数量 ÷ 全量并集，各类相加 = 整体一致率（页脚那句「分类别一致率
            # 合计」）。注意它不叫「全局一致率」——那是 ① 的另一项，分母是全量样本，
            # 比这个小，两个数同页并存，名字必须分得开
            "iou_weighted": c["iou_weighted"],
            "diff_count": c["diff_count"],
            # 差异金额 = 该类差异交易的 |amount| 之和，归因规则与 diff_count 同源
            "diff_amount": c["diff_amount"],
            "prev_date": (prev_cats[c["name"]].get("par_dt")
                          if c["name"] in prev_cats else None),
            # 上期的绝对值不再进载荷：表格只印 Δ 那一列
            "d_iou": delta(c["iou_rate"], prev_cats.get(c["name"]), "iou_rate"),
            "d_illion_coverage": delta(c["illion_coverage"], prev_cats.get(c["name"]),
                                       "illion_coverage"),
            "d_finv_coverage": delta(c["finv_coverage"], prev_cats.get(c["name"]),
                                     "finv_coverage"),
            "d_output_share": delta(c["output_share"], prev_cats.get(c["name"]),
                                    "output_share"),
            "d_finv_count": delta(c["finv_count"], prev_cats.get(c["name"]), "finv_count"),
            # 一致笔数的变化：各类一致笔数之和恒等于整体一致笔数，所以「这次整体
            # 一致率的变化主要来自哪个类别」可以直接相减归因
            "d_agree_count": delta(c["agree_count"], prev_cats.get(c["name"]), "agree_count"),
            "d_diff_count": delta(c["diff_count"], prev_cats.get(c["name"]), "diff_count"),
        } for c in cats],
        # 第五部分「业务板块」排序模式的顺序：板块名列表，顺序 = group 表行序，
        # 见 build_segments。只带顺序不带小计——板块这一层不展示任何自己的数字。
        "segments": [s["key"] for s in segments],
        "diffAmountTotal": facts[data_date]["amount"],
    }

    # 自检：三条聚合路径必须都回到同一个差异总数，且 Category 归因要能对上源表的
    # 「illion侧差异 + illion缺失」。对不上说明上游口径变了，宁可报错也不要出图。
    problems = []
    mis_sum = sum(t["count"] for t in payload["mismatchTypes"])
    cat_sum = sum(c["diff_count"] for c in cats)
    if mis_sum != payload["diffTotal"]:
        problems.append(f"差异类型合计 {mis_sum:,} != 差异总数 {payload['diffTotal']:,}")
    if cat_sum != payload["diffTotal"]:
        problems.append(f"Category 归属合计 {cat_sum:,} != 差异总数 {payload['diffTotal']:,}")
    # 差异金额与差异笔数由同一套归因规则算出（见 build_categories），各类相加必须等于
    # 页头规模卡那个「差异金额」（detail 全表的 |amount| 合计）。对不上说明两列分叉了，
    # 页面上就会出现一张「笔数对得上、金额对不上」的表。浮点求和顺序不同，取相对容差。
    amt_sum = sum(c["diff_amount"] for c in cats)
    if not math.isclose(amt_sum, payload["diffAmountTotal"], rel_tol=1e-9):
        problems.append(f"各类差异金额合计 {amt_sum:,.2f} "
                        f"!= 差异金额总计 {payload['diffAmountTotal']:,.2f}")
    got = {c["name"]: c["diff_count"] for c in cats}
    for _, r in rows_on(cat, data_date, "category").iterrows():
        name = str(r["Category"]).strip()
        expect = int(r["illion数量"] - r["一致数量"] + r["illion缺失"])
        if got.get(name) != expect:
            problems.append(f"{name}: 归因 {got.get(name)} != 源表口径 {expect}")

    # group 由 build_segments 回填，To-Do 每一行的「业务板块」显示的就是它；
    # 漏一个，那个类别展开后就会显示成空板块
    orphan = [c["name"] for c in cats if not c["group"]]
    if orphan:
        problems.append(f"{len(orphan)} 个 Category 不在任何业务板块: {'; '.join(orphan[:5])}")
    # 板块这一层现在直接决定第三部分怎么分块：除了差异总数，成员归属也要能对上账——
    # 每个类别必须恰好属于一个板块。少一个会有行落到「无板块」，多一个会被数两遍，
    # 而 group 还是 To-Do 每一行都要显示的字段。
    seg_sum = sum(s["diff_count"] for s in segments)
    if seg_sum != payload["diffTotal"]:
        problems.append(f"业务板块合计 {seg_sum:,} != 差异总数 {payload['diffTotal']:,}")
    seg_members = sum(s["member_count"] for s in segments)
    if seg_members != len(cats):
        problems.append(f"业务板块成员合计 {seg_members} != 类别数 {len(cats)}")

    # 全局一致率 = 该类一致数量 ÷ 全量并集，页面上写着「各类相加 = 整体一致率」，
    # 这里就按这个口径重算一遍：一类一致的交易只归属一个类别，所以各类之和必须
    # 精确等于 summary 的 交集÷并集（第一部分的那个数）。对不上说明有类别重叠或漏行。
    agree_sum = sum(c["agree_count"] for c in cats)
    agree_total = payload["summary"].get("agreement_rate_num")
    if agree_total is not None and agree_sum != agree_total:
        problems.append(f"各类一致数量合计 {agree_sum:,} != 整体一致数量 {agree_total:,.0f}")
    iou_w_sum = sum(c["iou_weighted"] for c in cats)
    iou_target = payload["summary"].get("coverage_adjusted_agreement")
    # 容差取 1e-6：36 个浮点数相加的误差在这个量级上（实测 ~4e-10），而任何一笔交易
    # 的真实出入都 >= 1/258371 ≈ 4e-6——容差再紧就会有假警报，再松就会漏掉真差异
    if iou_target is not None and abs(iou_w_sum - iou_target) > 1e-6:
        problems.append(f"各类全局一致率合计 {iou_w_sum:.6f} != 整体一致率 {iou_target:.6f}")

    # 输出分布占比（第四部分 Share 列）= 该类 finv 笔数 ÷ bscat 已分类总数。
    # 每笔交易只有一个 bscat 类别，所以各类相加必须等于 100%，且笔数之和必须等于
    # summary 里 finv 覆盖率的分子——对不上说明有个类别被漏掉或数了两遍。
    share_sum = sum(c["output_share"] or 0 for c in payload["categories"])
    if abs(share_sum - 1.0) > 1e-6:
        problems.append(f"各类输出分布占比合计 {share_sum:.6f} != 100%")
    finv_sum = sum(c["finv_count"] for c in cats)
    finv_num = payload["summary"].get("finv_coverage_num")
    if finv_num is not None and finv_sum != finv_num:
        problems.append(f"各类 finv 笔数合计 {finv_sum:,} != bscat 已分类 {finv_num:,.0f}")

    # 漏识别 / 多识别的笔数：各类漏判之和 == 整体漏识别笔数（错过一笔恰好记在它该
    # 属的那一类上），两侧未覆盖也各有各的账——「各类漏判 + 双方均无分类」必须等于
    # 「全量样本 − 该侧已判类」。这两条是 ② 那两条率线（漏识别 / 多识别）与两个排序档
    # 的全部依据，破了以后页面上的数仍然自洽地好看，只有把它们摆在一起才算得出来。
    miss_sum = sum(c["miss_count"] for c in cats)
    extra_sum = sum(c["extra_count"] for c in cats)
    miss_num = (metrics.get("miss") or {}).get("num")
    extra_num = (metrics.get("extra") or {}).get("num")
    both_empty = (metrics.get("unknown") or {}).get("num")
    total_rows = (metrics.get("unknown") or {}).get("den")
    if miss_num is not None and miss_sum != miss_num:
        problems.append(f"各类漏识别笔数合计 {miss_sum:,} != 整体漏识别 {int(miss_num):,}")
    if extra_num is not None and extra_sum != extra_num:
        problems.append(f"各类多识别笔数合计 {extra_sum:,} != 整体多识别 {int(extra_num):,}")
    if None not in (both_empty, total_rows) and finv_num is not None:
        uncov = int(total_rows) - int(finv_num)
        if miss_sum + int(both_empty) != uncov:
            problems.append(f"bscat 未覆盖 {uncov:,} != 各类漏判 {miss_sum:,} "
                            f"+ 双方均无分类 {int(both_empty):,}")
    illion_cov_num = payload["summary"].get("illion_coverage_num")
    if None not in (both_empty, total_rows) and illion_cov_num is not None:
        uncov = int(total_rows) - int(illion_cov_num)
        if extra_sum + int(both_empty) != uncov:
            problems.append(f"illion 未覆盖 {uncov:,} != 各类多判 {extra_sum:,} "
                            f"+ 双方均无分类 {int(both_empty):,}")

    # 类别 P 档必须是一份**划分**：36 类各归且只归一档，P0 就是那两个板块的全体成员。
    # 板块名或类别名拼错一个不会有别的症状——页面上只是少一枚徽标、排序档里少几行，
    # 不看不会发现，所以这里按**集合**逐项核一遍。
    lvl_of = {c["name"]: c.get("level") for c in cats}
    bad_lvl = sorted({str(v) for v in lvl_of.values() if v not in LEVELS})
    if bad_lvl:
        problems.append(f"类别 P 档不在 {LEVELS} 里: {bad_lvl}")
    miss_seg = [s for s in LEVEL_BY_SEGMENT if s not in {seg["key"] for seg in segments}]
    if miss_seg:
        problems.append(f"LEVEL_BY_SEGMENT 引用的板块不在数据里: {miss_seg}")
    p0_cats = {n for n, v in lvl_of.items() if v == "P0"}
    p0_want = {c["name"] for c in cats if c["group"] in LEVEL_BY_SEGMENT}
    if p0_cats != p0_want:
        problems.append(f"P0 的类别 {sorted(p0_cats)} "
                        f"!= 那两个板块的成员 {sorted(p0_want)}")
    miss_name = sorted(set(LEVEL_BY_NAME) - set(lvl_of))
    if miss_name:
        problems.append(f"LEVEL_BY_NAME 点名的类别不在 Category 列表里: {miss_name}")
    clash = sorted(set(LEVEL_BY_NAME) & p0_cats)
    if clash:
        problems.append(f"这些类别既在 P0 板块里、又被点名进 P1: {clash}")

    # 分类别历史是全部「变化」判据的唯一来源。这里核一下**当期那一天**确实按类别数
    # 铺全了（历史那几天也在表里，不能拿总行数来比）。
    now_rows = int((cat_hist_df["par_dt"].astype(str) == data_date).sum())
    if now_rows != len(cats):
        problems.append(f"类别历史本期 {now_rows} 行 != 类别数 {len(cats)}")
    if cat_history_days < 1:
        problems.append("类别历史期数为 0，说明这次没有写入")
    # 有上一期的话，Δ 至少要能取到一致率那一列——全为 None 说明列名对不上
    if prev_cats and not any(c["d_iou"] is not None for c in payload["categories"]):
        problems.append("有上一期历史但各类一致率 Δ 全为空，检查列名")

    for c in cats:
        if sum(c["type_mix"].values()) != c["diff_count"]:
            problems.append(f"{c['name']}: 差异构成 {sum(c['type_mix'].values())} "
                            f"!= 差异笔数 {c['diff_count']}")

    # To-Do 每一项都得能对回 Category 表（全局项除外——它本来就不属于任何类别，
    # 归属名必须是「全体类别」）。对不上说明规则引擎引用了不存在的类别，
    # 页面上会出现一条没有依据的待办。
    by_name = {c["name"]: c for c in cats}
    for t in items:
        if t["scope"] == "global":
            if t["name"] != GLOBAL_CATEGORY:
                problems.append(f"全局待办的归属 {t['name']!r} != {GLOBAL_CATEGORY!r}")
            continue
        c = by_name.get(t["name"])
        if c is None:
            problems.append(f"To-Do「{t['name']}」不在 Category 列表里")
        elif abs((c["iou_rate"] or 0) - (t["iou_rate"] or 0)) > 1e-9:
            problems.append(f"To-Do「{t['name']}」一致率与 Category 表不一致")

    # 去重后的类别差异合计才是有意义的那个数：一个类别命中两条规则时逐条相加会
    # 把它数两遍，得出一个比差异总数还大的数——所以这条上限检查必须还在。
    if payload["todo"]["diff_count"] > payload["diffTotal"]:
        problems.append(f"To-Do 差异合计 {payload['todo']['diff_count']:,} "
                        f"超过差异总数 {payload['diffTotal']:,}")

    # 问题诊断要能层层对上账：类别 → 类型 → 流向逐层相加都必须回到该类的差异笔数，
    # 对不上就是「点开看到的是另一个数」，比不显示更糟
    for name, node in payload["catDetails"].items():
        c = by_name.get(name)
        if c is None:
            problems.append(f"明细里的类别「{name}」不在 Category 列表里")
        elif node["count"] != c["diff_count"]:
            problems.append(f"{name}: 明细流向合计 {node['count']} != 差异笔数 {c['diff_count']}")
        t_sum = sum(t["count"] for t in node["types"])
        if t_sum != node["count"] - node["unlabeled"]:
            problems.append(f"{name}: 明细类型合计 {t_sum} != 明细笔数 "
                            f"{node['count']}（未标注 {node['unlabeled']}）")
        if node["unlabeled"]:
            problems.append(f"{name}: 明细里有 {node['unlabeled']} 笔未标注差异类型")
        if sum(node["type_mix"].values()) != t_sum:
            problems.append(f"{name}: 明细差异构成 {sum(node['type_mix'].values())} "
                            f"!= 明细类型合计 {t_sum}")

    # 全局一致率 = 标签相同的样本数 ÷ 全量样本数。分母必须是「全量样本」——换成并集
    # 就退化成 ① 那张「一致率」卡，两个指标长得一样，而它们回答的是两个问题。
    ga = (payload["metrics"].get("global_agreement") or {})
    if ga.get("num") is None or not ga.get("den"):
        problems.append("全局一致率缺分子或分母：summary 里没有一致笔数 / 全量样本")
    elif abs((ga.get("value") or 0) - ga["num"] / ga["den"]) > 1e-9:
        problems.append(f"全局一致率 {ga.get('value')} != {ga['num']} / {ga['den']}")

    # ② 那两个覆盖差排序档（「覆盖差 · bscat 低 / 高」）成立的前提：两个覆盖率的分母
    # 都是全量样本，对每一类都完全相同，所以各类的差加起来必须一分不差地等于整体那个差
    # ——「按各类差值排」就等于「按对整体差值的贡献排」。哪天有人给分母换了个每类不同
    # 的东西（比如改用本类并集），两条档会静默地排错序，页面上看不出任何异样。
    # 这条是结构性的：18 期历史逐日核过，不是当期数据的巧合。
    bsc_cov = (payload["metrics"].get("bscat_coverage") or {}).get("value")
    ill_cov = (payload["metrics"].get("illion_coverage") or {}).get("value")
    cat_gap_sum = sum((c.get("coverage_gap") or 0) for c in payload["categories"])
    if bsc_cov is None or ill_cov is None:
        problems.append("整体覆盖率差算不出来：① 缺了两侧覆盖率中的一个")
    elif abs(cat_gap_sum - (bsc_cov - ill_cov)) > 1e-9:
        problems.append(f"各类覆盖率差之和 {cat_gap_sum} != 整体覆盖率差 {bsc_cov - ill_cov}"
                        f"（按差值排序就不再等于按贡献排序，② 的两个覆盖差档会排错）")

    # 最有价值的一条：照规则表**独立重跑一遍**两段规则，把期望的
    # (类别, 规则 ID) 集合与载荷里的比对，多出和漏掉两侧都报。
    # 这是「待办只能由规则产生」这句话的机器版本——有谁绕过规则塞了一条，
    # 或者抑制关系写反了，都会在这里当场露出来。
    #
    # 独立在「就地重算」这件事上：这里不调 evaluate_rules / evaluate_alerts，
    # 而是照着同一份判据把 (类别, 规则) 再列一遍。调那两个函数等于把实现抄一遍，
    # 它错在哪，重跑就错在哪。
    expect: set[tuple[str, str]] = set()
    hit_seen: dict[str, set[str]] = {}

    def _expect(rule: dict, name: str, ok: bool) -> None:
        if not ok:
            return
        if any(name in hit_seen.get(s, ()) for s in rule.get("suppressedBy", [])):
            return
        hit_seen.setdefault(rule["id"], set()).add(name)
        expect.add((name, rule["id"]))

    for rule in MONITORING_RULES:
        if not rule["enabled"]:
            continue
        if rule["scope"] == "global":
            _expect(rule, GLOBAL_CATEGORY,
                    _day_result(rule, _rows_for(rule, global_row(metrics)), 0) is True)
            continue
        for c in cats:
            _expect(rule, c["name"],
                    _day_result(rule, _rows_for(rule, c), 0) is True)
    # 按日规则的判据来自分类别序列，与上面那份 cats 不是同一组数——序列里多了历史，
    # 所以这里必须用载荷里那份序列重判，而不是复用当期这一行。
    ser = payload["catSeries"]
    end_k = len(ser["dates"]) - 1
    if end_k >= 0:
        global_ser = cat_series.get("global")
        for rule in ALERT_RULES:
            if not rule["enabled"]:
                continue
            if rule["scope"] == "global":
                if global_ser is None:
                    problems.append(f"{rule['id']}: 全局规则没有整体序列，判不了")
                    continue
                vals = rule_vals(rule, global_ser)
                st = rule_streak(rule, vals, end_k)
                _expect(rule, GLOBAL_CATEGORY,
                        st["today"] is True and st["days"] >= rule["window"])
                continue
            for name, s in ser["series"].items():
                vals = rule_vals(rule, s)
                st = rule_streak(rule, vals, end_k)
                _expect(rule, name, st["today"] is True and st["days"] >= rule["window"])
    got_todo = {(t["name"], t["ruleId"]) for t in items}
    if got_todo != expect:
        problems.append("To-Do 与规则重跑不符: 多出 "
                        f"{sorted(got_todo - expect)}，漏掉 {sorted(expect - got_todo)}")

    # 预警的「连续天数」必须与序列逐日重数出来的结果一致。这个数会直接印在卡片上
    # （「连续 3 期」），算错一天没人看得出来——重数一遍是最便宜的验证。
    for t in items:
        if t.get("kind") != "alert":
            continue
        rule = RULE_BY_ID.get(t["ruleId"])
        if rule is None:
            problems.append(f"预警 {t['name']}/{t['ruleId']} 找不到规则")
            continue
        # 全局条目按规则域取序列：类别序列里没有「全体类别」这一块。
        s = (cat_series.get("global") if rule["scope"] == "global"
             else ser["series"].get(t["name"]))
        if s is None:
            problems.append(f"预警 {t['name']}/{t['ruleId']} 找不到序列")
            continue
        vals = rule_vals(rule, s)
        want = 0
        k = end_k
        while k >= 0 and _day_result(rule, vals, k) is True:
            want += 1
            k -= 1
        if t["consecutiveDays"] != want:
            problems.append(f"{t['name']}/{t['ruleId']}: 卡片上的连续天数 "
                            f"{t['consecutiveDays']} != 重数出来的 {want}")
        if len(t["streakDates"]) != want or t["streakDates"][-1] != ser["dates"][end_k]:
            problems.append(f"{t['name']}/{t['ruleId']}: 连续期日期 {t['streakDates']} "
                            f"与重数出来的 {want} 期对不上")

    # 规则表本身的完整性。加载时已经校验过一遍（写错就不出图），这里再核一次是照
    # **载荷里那份**核：页脚展示的判据、卡片上写的阈值都从载荷来，配置对而载荷被
    # 改错，页面上写得再对也没用。
    ids = [r["id"] for r in MONITORING_RULES + ALERT_RULES]
    dup_ids = sorted({i for i in ids if ids.count(i) > 1})
    if dup_ids:
        problems.append(f"规则 ID 重复: {dup_ids}")
    metric_keys = {k for k, *_ in METRIC_DEFS}
    for r in MONITORING_RULES + ALERT_RULES:
        if r["issueType"] not in ENUMS["issueType"]:
            problems.append(f"{r['id']}: issueType {r['issueType']!r} 不在枚举里")
        if r["recommendedAction"] not in ENUMS["recommendedAction"]:
            problems.append(f"{r['id']}: recommendedAction {r['recommendedAction']!r} 不在枚举里")
        if not set(r["metrics"]) <= metric_keys:
            problems.append(f"{r['id']}: metrics {r['metrics']} 里有不存在的指标行")
        # 全局规则必须有一个看得见的角标位：tile 不在 ① 六张卡里，命中后就只剩
        # todo_status.csv 里一行，页面上完全看不到。
        if r["scope"] == "global" and r.get("tile") not in TILE_METRIC_KEYS:
            problems.append(f"{r['id']}: 全局规则的 tile {r.get('tile')!r} "
                            f"不在 ① 指标卡名单 {list(TILE_METRIC_KEYS)} 里")
        # 条件文案里的占位符与 thresholds 必须严格同号：少了占位符，那个阈值就是
        # 写了个没人看的数；多一个占位符，页面上引用了不存在的阈值会当场炸。
        holes = set(re.findall(r"\{(\w+)(?::[^}]*)?\}", r["condition"]))
        if holes != set(r["thresholds"]):
            problems.append(f"{r['id']}: 条件占位符 {sorted(holes)} "
                            f"与阈值 {sorted(r['thresholds'])} 不一致")
    if payload["todo"]["rules"] != rule_thresholds():
        problems.append("载荷里的阈值字典与规则表不同源")
    if [r["id"] for r in payload["rulesTable"]] != ids:
        problems.append("页脚规则表与规则表的顺序 / 条数不一致")

    # 分类别序列的形状：日期轴、每类每列的长度必须严丝合缝。序列是 36 块小图与
    # 全部预警的唯一数据源，短一截不会报错，只会让某几类的小图从某天起断掉。
    if ser["dates"] != sorted(ser["dates"]):
        problems.append("分类别序列的日期轴不是升序")
    if ser["dates"] and ser["dates"][-1] != data_date:
        problems.append(f"分类别序列的最后一天 {ser['dates'][-1]} != 监控日 {data_date}")
    if len(ser["series"]) != len(cats):
        problems.append(f"分类别序列有 {len(ser['series'])} 类 != 类别数 {len(cats)}")
    for name, s in ser["series"].items():
        bad = [col for col in ALERT_SERIES_COLS if len(s.get(col) or []) != len(ser["dates"])]
        if bad:
            problems.append(f"{name}: 序列列 {bad} 的长度 != 日期轴 {len(ser['dates'])}")
            break
        # 最后一天必须与 Category 表逐字段一致，否则「图上最后一点」与「表格里那个数」
        # 是两个数——序列铺错一天就是这个症状，而且只有最后一天是当期现算的。
        c = by_name.get(name) or {}
        for col in ("iou_rate", "illion_coverage", "finv_coverage",
                    "diff_count", "diff_amount", "miss_rate", "extra_rate"):
            a, b = s[col][-1], c.get(col)
            if a is None or b is None or abs(float(a) - float(b)) > 1e-9:
                problems.append(f"{name}: 序列末点 {col}={a} != Category 表 {b}")
                break

    # 枚举。这里查的全是「名单本身有没有毛病」——真的取值错误在页面侧由
    # normalizeEnum 兜底，但兜底会把错值变成 Other，反而看不出名单写错了。
    for f, vals in ENUMS.items():
        if not vals:
            problems.append(f"枚举 {f} 是空的")
        dup = sorted({v for v in vals if vals.count(v) > 1})
        if dup:
            problems.append(f"枚举 {f} 有重复值: {'; '.join(dup)}")
    if set(ENUM_FALLBACK) != set(ENUMS):
        problems.append(f"ENUM_FALLBACK 与 ENUMS 字段不符: "
                        f"{sorted(set(ENUM_FALLBACK) ^ set(ENUMS))}")
    for f, fb in ENUM_FALLBACK.items():
        if fb not in ENUMS.get(f, []):
            problems.append(f"枚举 {f} 的落点 {fb!r} 不在名单里")
    # 差异类型是唯一「页面已有业务含义、只需补受控值」的一处：三个 key 一个都不能少，
    # 少一个就会有整类差异在页面上没有受控值可用
    if set(DIFF_TYPE_ENUM) != {k for k, *_ in MISMATCH_TYPES}:
        problems.append("DIFF_TYPE_ENUM 的 key 与 MISMATCH_TYPES 不一致")
    for k, v in DIFF_TYPE_ENUM.items():
        if v not in ENUMS["differenceType"]:
            problems.append(f"差异类型 {k} 的受控值 {v!r} 不在枚举里")
    # 这里查包含不查相等：Other 是落点不是差异类型，不该出现在实际差异构成里
    if not {t["enum"] for t in payload["mismatchTypes"]} <= set(ENUMS["differenceType"]):
        problems.append("载荷里的差异类型受控值不在枚举里")
    for t in items:
        if t["issueType"] not in ENUMS["issueType"]:
            problems.append(f"{t['name']}/{t['ruleId']}: 问题类型 {t['issueType']!r} 不在枚举里")
        if t["recommendedAction"] not in ENUMS["recommendedAction"]:
            problems.append(f"{t['name']}/{t['ruleId']}: 推荐动作 "
                            f"{t['recommendedAction']!r} 不在枚举里")
        if t["status"] not in ENUMS["taskStatus"]:
            problems.append(f"{t['name']}/{t['ruleId']}: 任务状态 {t['status']!r} 不在枚举里")
        if t["sourceDate"] < t["firstTriggerDate"]:
            problems.append(f"{t['name']}/{t['ruleId']}: 最近触发 {t['sourceDate']} "
                            f"早于首次触发 {t['firstTriggerDate']}")

    # ① 七张卡与趋势本期行同源，逐项核一遍：卡片上那几个数是页面上最先被读到的，
    # 和图表对不上就会让人以为是数据坏了。逐项比对是唯一的防线。
    cur_row = find_trend_row(trend_hist, data_date)
    for key, _label, col, _dir in METRIC_DEFS:
        mv = (payload["metrics"].get(key) or {}).get("value")
        cv = cur_row.get(col) if cur_row else None
        cv = None if cv is None or pd.isna(cv) else float(cv)
        if (mv is None) != (cv is None) or (mv is not None and abs(float(mv) - cv) > 1e-9):
            problems.append(f"①「{key}」的值 {mv} != 趋势本期行 {col}={cv}")
    # unknown_rate 是新加的口径，分子分母都带上，这里按定义重算一遍——
    # 写错了不会有别的症状，只会在页面上显示一个没人复核过的百分比。
    if cur_row is not None:
        ur, be, tot = (cur_row.get("unknown_rate"), cur_row.get("both_empty_rows"),
                       cur_row.get("total_rows"))
        if ur is not None and tot and float(tot) > 0:
            if abs(float(ur) - float(be) / float(tot)) > 1e-6:
                problems.append(f"unknown_rate {ur} != 双方均无分类 {be} ÷ 总样本 {tot}")

    # ② 变化检测：方向必须在枚举里、能按 delta 重算，取值必须回到趋势行。
    # 算错不会有别的症状——只会让一个方向的结论整体反过来，而那正是要读的东西。
    if payload["prevDate"] is None and payload["changes"]:
        problems.append("没有上一期却算出了变化行")
    prow = prev_trend_row(trend_hist, data_date)
    for row in payload["changes"]:
        if row["direction"] is not None:
            if row["direction"] not in ENUMS["changeDirection"]:
                problems.append(f"变化方向 {row['direction']!r} 不在枚举里")
            elif row["direction"] != change_direction(row["delta"]):
                problems.append(f"{row['key']}: 方向 {row['direction']} 与 delta 重算不符")
        # worsened 是页面上「走坏」那句话的唯一判据，算错会印出方向自相矛盾的句子
        # （覆盖率涨了却写「走坏」），而且不会有别的症状。按 delta + dir 重算一遍。
        if row["delta"] is None:
            if row["worsened"]:
                problems.append(f"{row['key']}: 没有变化幅度却判成走坏")
        else:
            d = row["delta"]
            want = ((row["dir"] == "up_good" and d < 0)
                    or (row["dir"] == "down_good" and d > 0)) and abs(d) * 100 >= CHANGE_BAD_PP
            if row["worsened"] != want:
                problems.append(f"{row['key']}: worsened={row['worsened']} 与 delta 重算不符")
        col = {k: c for k, _l, c, _d in METRIC_DEFS}[row["key"]]
        for want, src in ((row["current"], cur_row), (row["previous"], prow)):
            got = src.get(col) if src else None
            got = None if got is None or pd.isna(got) else float(got)
            if (want is None) != (got is None) or (want is not None and abs(want - got) > 1e-9):
                problems.append(f"{row['key']}: 取值 {want} != 趋势行 {col}={got}")

    # ④ 的「对比期」逐行写的是类别自己那一期的日期。它要么和整体那一期相同，
    # 要么为空（当期新出现的类别）。两边取到不同的期，Δ 就不是同一段时间的变化了。
    for c in payload["categories"]:
        if c["prev_date"] is not None and c["prev_date"] != payload["prevDate"]:
            problems.append(f"{c['name']}: 对比期 {c['prev_date']} "
                            f"!= 整体对比期 {payload['prevDate']}")

    if problems:
        log("自检未通过:")
        for p in problems[:12]:
            log(f"  ✗ {p}")
        if len(problems) > 12:
            log(f"  ... 另有 {len(problems)-12} 项")
        if args.strict:
            raise SystemExit("自检失败，已中止（去掉 --strict 可强制出图）")
    else:
        # 命中项的正负差各自的个数报出来：全挤在同一侧时一眼能看见，
        # 不用翻页去数——单向的筛选 bug 就是这样被发现的
        npos = sum(1 for t in items if (t.get("coverage_gap") or 0) > 0)
        nneg = sum(1 for t in items if (t.get("coverage_gap") or 0) < 0)
        log(f"自检通过: 差异总数 {payload['diffTotal']:,}，"
            f"待办 {len(items)} 项"
            + (f"（全局 {todo['global']}）" if todo["global"] else "")
            + f"，覆盖 {todo['categories']} 个类别，正差 {npos} / 负差 {nneg}，"
            f"变化 {len(payload['changes'])} 行，"
            f"明细 {len(cat_details)} 个类别")

    tpl = tpl_path.read_text(encoding="utf-8")
    html = render(payload, tpl)

    out_html = out_dir / f"bscat_monitor_{datetime.now():%Y%m%d_%H%M%S}.html"
    out_html.write_text(html, encoding="utf-8")
    log(f"生成 {out_html}  ({len(html.encode('utf-8'))/1024/1024:.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
