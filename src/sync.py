#!/usr/bin/env python3
"""同步 BSCAT 监控看板数据：执行 sync.sql 的 7 个查询块，写入 input/。

前 5 块是看板底表，后 2 块是收入/负债流汇总（只随 zip 归档，monitor.py 不读）。
monitor.py 默认在启动时调用 sync_all() 自动同步；也可以单独运行本脚本：
    python src/sync.py

数据库连接由外部 hive_config.query_to_df(sql) 提供，本脚本不负责建连。
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")


SRC_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SRC_DIR.parent
SQL_FILE = SRC_DIR / "sync.sql"
INPUT_DIR = PROJECT_DIR / "input"

# 块号 → 输出文件。顺序必须与 SQL 中 SELECT 的顺序一一对应。
QUERY_SPECS = (
    {"id": "1", "file": "risk_000008_dash_summary.csv",  "title": "汇总指标"},
    {"id": "2", "file": "risk_000008_dash_trend.csv",    "title": "逐日趋势"},
    {"id": "3", "file": "risk_000008_dash_group.csv",    "title": "业务板块"},
    {"id": "4", "file": "risk_000008_dash_category.csv", "title": "分类别指标"},
    {"id": "5", "file": "risk_000008_dash_detail.csv",   "title": "逐笔明细"},
    {"id": "6", "file": "risk_000008_log_income_summary.csv",
     "title": "收入流汇总（仅归档）"},
    {"id": "7", "file": "risk_000008_log_liability_summary.csv",
     "title": "负债流汇总（仅归档）"},
)

_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", flags=re.S)
_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
# Evolve 圈选导出的值直接拼进 SQL 字面量，只放行纯 ID / 日期的形状。
_APP_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def split_statements(sql_text):
    """去掉注释后按分号拆分；返回 SQL 语句列表。"""
    body = _BLOCK_COMMENT_RE.sub(" ", sql_text)
    body = _LINE_COMMENT_RE.sub(" ", body)
    return [part.strip() for part in body.split(";") if part.strip()]


def build_queries():
    statements = split_statements(SQL_FILE.read_text(encoding="utf-8"))
    if len(statements) != len(QUERY_SPECS):
        raise ValueError(
            f"SQL 查询数 {len(statements)} 与 QUERY_SPECS 的 {len(QUERY_SPECS)} 条不一致；"
            "请同步更新 sync.py 的文件清单"
        )
    return [{**spec, "sql": statement} for spec, statement in zip(QUERY_SPECS, statements)]


def load_query_fn():
    try:
        from hive_config import query_to_df
    except ImportError:
        print("未找到 hive_config.py：请把提供 query_to_df(sql) 的模块放到本脚本同目录或 PYTHONPATH 中。")
        return None
    return query_to_df


def run_query_to_csv(sql, path, query_fn):
    """执行一条查询并写入 path；返回 (行数, 列名)。失败时抛异常。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # 删掉上一次的文件，失败时目录里不会留下看似成功的旧结果。
    if path.exists():
        path.unlink()

    df = query_fn(sql)
    if not isinstance(df, pd.DataFrame):
        raise ValueError("查询未返回 DataFrame")
    if len(df) == 0:
        print("警告 | 查询返回 0 行，请确认底表是否有数据")

    temporary = path.with_suffix(".csv.tmp")
    try:
        df.to_csv(temporary, index=False, encoding="utf-8-sig")
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()
    return int(len(df)), list(df.columns)


def build_evolve_transactions_sql(application_ids, sample_date):
    """圈选 application 当天的全部逐笔。没有合法 application_id 时返回 None。"""
    ids = sorted({str(a).strip() for a in application_ids
                  if _APP_ID_RE.match(str(a).strip())})
    if not ids:
        return None
    sample_date = str(sample_date).strip()
    if not _DATE_RE.match(sample_date):
        raise ValueError(f"sample_datetime {sample_date!r} 不是 yyyy-MM-dd")
    in_list = ", ".join(f"'{a}'" for a in ids)
    return (
        "SELECT * FROM model.tmp_eliam_risk_000008_log_transactions\n"
        f"WHERE sample_datetime = '{sample_date}'\n"
        f"  AND application_id IN ({in_list})"
    )


def export_evolve_transactions(application_ids, sample_date, path):
    """导出圈选的逐笔到 path；返回行数。无合法 ID 返回 0，失败抛异常。"""
    sql = build_evolve_transactions_sql(application_ids, sample_date)
    if sql is None:
        return 0
    query_fn = load_query_fn()
    if query_fn is None:
        raise RuntimeError("未找到 hive_config.query_to_df")
    rows, _columns = run_query_to_csv(sql, path, query_fn)
    return rows


def sync_all(input_dir: Path = INPUT_DIR) -> int:
    """执行全部查询并写入 input_dir；返回 0 表示全部成功。"""
    try:
        queries = build_queries()
    except ValueError as exc:
        print(f"同步中止 | {exc}")
        return 2

    query_fn = load_query_fn()
    if query_fn is None:
        return 2

    input_dir = Path(input_dir)
    input_dir.mkdir(parents=True, exist_ok=True)

    failed = []
    for index, query in enumerate(queries, start=1):
        path = input_dir / query["file"]
        print(f"[{index}/{len(queries)}] {query['id']}. {query['title']} → input/{path.name}")
        try:
            rows, columns = run_query_to_csv(query["sql"], path, query_fn)
            print(f"成功 | {rows} 行 × {len(columns)} 列")
        except Exception as exc:
            failed.append(query["id"])
            print(f"失败 | {exc}")

    print(f"同步完成：成功 {len(queries) - len(failed)} / 失败 {len(failed)}")
    return 1 if failed else 0


def main() -> int:
    return sync_all()


if __name__ == "__main__":
    raise SystemExit(main())
