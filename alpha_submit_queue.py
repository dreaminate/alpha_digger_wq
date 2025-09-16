# -*- coding: utf-8 -*-
"""
alpha_submit_queue.py
从本地记录中自动挑选待提交的 alpha_id，并在开始前询问数量。
- 默认读取: RECORDS_PATH / 'submitable_alpha.csv'
- 兼容 'id' 或 'alpha_id' 列
- 自动排除本地“已提交”记录中出现过的 id（支持 submitted.csv）
- 不按 status 过滤（require_active 参数保留但不生效）
- 可交互询问要提交多少，或通过参数直接指定

用法：
from alpha_submit_queue import pick_submittable_alpha_ids
submittable_alphas = pick_submittable_alpha_ids(RECORDS_PATH, ask=True, default_n=None)
"""

import os
import pandas as pd
from typing import List, Optional, Sequence

pd.set_option('display.max_colwidth', 120)

SUBMITABLE_FILENAME = "submitable_alpha.csv"
SUBMITTED_CANDIDATES = ["submitted.csv"]  # 必须是列表
ID_COL_CANDIDATES = ["alpha_id", "id", "alphaId"]


def _read_csv_safely(path: str) -> Optional[pd.DataFrame]:
    """更鲁棒的 CSV 读取：自动嗅探分隔符；失败返回 None。"""
    if not os.path.exists(path):
        return None
    for kwargs in ({"engine": "python", "sep": None},
                   {"engine": "python", "sep": ","},
                   {"engine": "python", "sep": "\t"},
                   {"engine": "python", "sep": ";"}):
        try:
            df = pd.read_csv(path, **kwargs)
            if isinstance(df, pd.DataFrame) and df.shape[1] >= 1:
                # 统一去掉列名两侧空格
                df.columns = [str(c).strip() for c in df.columns]
                return df
        except Exception:
            continue
    return None


def _detect_id_col(df: pd.DataFrame) -> Optional[str]:
    for c in ID_COL_CANDIDATES:
        if c in df.columns:
            return c
    # 再尝试大小写不一致的情形
    low = {c.lower(): c for c in df.columns}
    for c in ID_COL_CANDIDATES:
        if c.lower() in low:
            return low[c.lower()]
    return None


def _load_ids_from_csv(path: str) -> List[str]:
    df = _read_csv_safely(path)
    if df is None:
        return []
    col = _detect_id_col(df)
    if not col:
        return []
    return (
        df[col]
        .astype(str)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA})
        .dropna()
        .unique()
        .tolist()
    )


def _gather_submitted_ids(records_path: str) -> set:
    submitted_ids = set()
    for fname in SUBMITTED_CANDIDATES:
        fpath = os.path.join(records_path, fname)
        submitted_ids.update(_load_ids_from_csv(fpath))
    return submitted_ids


def pick_submittable_alpha_ids(
    records_path: str,
    submitable_filename: str = SUBMITABLE_FILENAME,
    ask: bool = True,
    default_n: Optional[int] = None,
    require_active: bool = False,  # 兼容参数：保留但不生效
    sort_by: Sequence[str] = ("dateCreated", "dateSubmitted", "timestamp"),
    descending: bool = True,
    random_pick: bool = False,
    random_seed: Optional[int] = None,
) -> List[str]:
    """返回准备提交的 alpha_id 列表（只返回字符串列表）"""

    submitable_path = os.path.join(records_path, submitable_filename)
    df = _read_csv_safely(submitable_path)
    if df is None:
        print(f"[alpha_queue] 找不到或无法读取 {submitable_path}，返回空列表。")
        return []

    # 统一 id 列为 alpha_id
    id_col = _detect_id_col(df)
    if not id_col:
        print(f"[alpha_queue] {submitable_path} 中未找到 id 列（支持 {ID_COL_CANDIDATES}），返回空列表。")
        return []
    if id_col != "alpha_id":
        df = df.rename(columns={id_col: "alpha_id"})

    # 清洗 & 去重
    df["alpha_id"] = (
        df["alpha_id"].astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA})
    )
    df = df.dropna(subset=["alpha_id"]).drop_duplicates(subset=["alpha_id"])

    # ——不做任何 status 过滤——

    # 排除已提交
    submitted_ids = _gather_submitted_ids(records_path)
    if submitted_ids:
        df = df[~df["alpha_id"].isin(submitted_ids)]

    # 排序（若存在可用列）
    for key in sort_by:
        if key in df.columns:
            try:
                parsed = pd.to_datetime(df[key], errors="coerce")
                if parsed.notna().any():
                    df = (
                        df.assign(_key=parsed)
                          .sort_values("_key", ascending=not descending, kind="mergesort", na_position="last")
                          .drop(columns="_key")
                    )
                else:
                    df = df.sort_values(key, ascending=not descending, kind="mergesort")
            except Exception:
                df = df.sort_values(key, ascending=not descending, kind="mergesort")
            break  # 只用第一个命中的排序键

    ids = df["alpha_id"].tolist()

    # 随机抽取（可选）
    if random_pick and len(ids) > 1:
        ids = pd.Series(ids).sample(frac=1.0, random_state=random_seed).tolist()

    # 询问数量/截断
    total = len(ids)
    if total == 0:
        print("[alpha_queue] 没有可提交的 alpha。")
        return []

    if ask:
        default_hint = "全部" if default_n is None else str(default_n)
        while True:
            raw = input(f"请输入要提交的数量（回车默认 {default_hint}，也可输入 all）：").strip().lower()
            if raw in ("",):
                n = total if default_n is None else min(default_n, total)
                break
            if raw in ("all", "a", "full"):
                n = total
                break
            try:
                n = int(raw)
                if n <= 0:
                    print("请输入正整数。")
                    continue
                n = min(n, total)
                break
            except ValueError:
                print("无效输入，请输入正整数或 all。")
    else:
        n = total if default_n is None else min(max(1, int(default_n)), total)

    return ids[:n]
