# -*- coding: utf-8 -*-
"""
alpha_submit_queue.py
从本地记录中自动挑选待提交的 alpha_id，并在开始前询问数量。
- 默认读取: RECORDS_PATH / 'submitable_alpha.csv'
- 兼容 'id' 或 'alpha_id' 列
- 自动排除本地“已提交”记录中出现过的 id（多文件名兼容）
- 可选过滤 status=ACTIVE
- 可交互询问要提交多少，或通过参数直接指定

用法（在你的 submit_alpha 脚本里）:
from alpha_submit_queue import pick_submittable_alpha_ids
submittable_alphas = pick_submittable_alpha_ids(RECORDS_PATH, ask=True, default_n=None)
"""

import os
import sys
import pandas as pd
from typing import List, Optional, Sequence

# 让 DataFrame 显示不影响逻辑
pd.set_option('display.max_colwidth', 120)


SUBMITABLE_FILENAME = "submitable_alpha.csv"

# 兼容你可能已有的“已提交记录”文件名（只要存在就会读取，取并集）
SUBMITTED_CANDIDATES =  "submitted.csv"
    


ID_COL_CANDIDATES = ["id", "alpha_id", "alphaId"]


def _detect_id_col(df: pd.DataFrame) -> Optional[str]:
    for c in ID_COL_CANDIDATES:
        if c in df.columns:
            return c
    return None


def _load_ids_from_csv(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    try:
        df = pd.read_csv(path)
    except Exception:
        # 某些文件可能是空/坏，直接忽略
        return []
    col = _detect_id_col(df)
    if not col:
        return []
    return (
        df[col]
        .astype(str)
        .str.strip()
        .dropna()
        .replace({"nan": None})
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
    require_active: bool = True,
    sort_by: Sequence[str] = ("dateCreated", "dateSubmitted", "timestamp"),
    descending: bool = True,
    random_pick: bool = False,
    random_seed: Optional[int] = None,
) -> List[str]:
    """
    返回准备提交的 alpha_id 列表。

    参数说明：
    - records_path: 你的 RECORDS_PATH
    - submitable_filename: 可提交清单 CSV 文件名（默认 'submitable_alpha.csv'）
    - ask: 是否在开始前交互询问数量
    - default_n: 交互默认数量（None 表示默认全选）
    - require_active: 若存在 'status' 列，则仅保留 status == 'ACTIVE'
    - sort_by: 若文件包含这些列，则按优先顺序排序（第一个存在的列生效）
    - descending: 排序是否倒序
    - random_pick: 是否随机抽取
    - random_seed: 随机种子
    """
    submitable_path = os.path.join(records_path, submitable_filename)
    if not os.path.exists(submitable_path):
        print(f"[alpha_queue] 找不到 {submitable_path}，返回空列表。")
        return []

    try:
        df = pd.read_csv(submitable_path)
    except Exception as e:
        print(f"[alpha_queue] 读取失败: {submitable_path} -> {e}")
        return []

    id_col = _detect_id_col(df)
    if not id_col:
        print(f"[alpha_queue] {submitable_path} 中未找到 id 列（支持 {ID_COL_CANDIDATES}），返回空列表。")
        return []

    # 去重 & 清洗
    df[id_col] = df[id_col].astype(str).str.strip()
    df = df.dropna(subset=[id_col]).drop_duplicates(subset=[id_col])

    # 仅保留 ACTIVE（如果有 status 列）
    if require_active and "status" in df.columns:
        df = df[df["status"].astype(str).str.upper() == "ACTIVE"]

    # 排除“已提交”的 id（从多个可能文件名汇总）
    submitted_ids = _gather_submitted_ids(records_path)
    if submitted_ids:
        df = df[~df[id_col].isin(submitted_ids)]

    # 排序：按第一个存在的 sort_by 列来排（常见是 dateCreated）
    for key in sort_by:
        if key in df.columns:
            try:
                # 尝试当时间戳解析；失败则按字符串排序
                df = df.copy()
                df[key] = pd.to_datetime(df[key], errors="ignore")
                df = df.sort_values(by=key, ascending=not descending, kind="mergesort")
            except Exception:
                df = df.sort_values(by=key, ascending=not descending, kind="mergesort")
            break  # 只用第一个命中的列

    # 形成候选 id 列表
    ids = df[id_col].tolist()
    total = len(ids)

    if total == 0:
        print("[alpha_queue] 没有可提交的 alpha。")
        return []

    # 随机抽取（如开启）
    if random_pick:
        rng = pd.Series(ids)
        ids = rng.sample(frac=1.0, random_state=random_seed).tolist()

    # 询问数量
    if ask:
        print(f"[alpha_queue] 发现可提交 {total} 个 alpha。")
        if default_n is None:
            default_hint = "全部"
        else:
            default_hint = str(default_n)

        while True:
            raw = input(f"请输入要提交的数量（回车默认 {default_hint}，也可输入 all）：").strip().lower()
            if raw in ("", ):
                # 默认
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
        # 非交互：用 default_n（None=全部）
        n = total if default_n is None else min(default_n, total)

    selected = ids[:n]
    print(f"[alpha_queue] 本次将提交 {len(selected)}/{total} 个。")
    return selected
