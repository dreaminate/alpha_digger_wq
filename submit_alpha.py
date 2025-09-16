# -*- coding: utf-8 -*-
import datetime
import os
import time
import pandas as pd

from alpha_submit_queue import pick_submittable_alpha_ids
from config import RECORDS_PATH
from machine_lib import login

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', 100)


# ========== 工具函数 ==========
def _parse_self_corr_fail(res):
    """从返回里检查 SELF_CORRELATION 是否 FAIL；返回 (is_fail, value)"""
    try:
        j = res.json()
        checks = (((j or {}).get("is") or {}).get("checks")) or []
    except Exception:
        return False, None
    val = None
    for c in checks:
        name = str(c.get("name", "")).strip().upper()
        if name == "SELF_CORRELATION":
            result = str(c.get("result", "")).strip().upper()
            v = c.get("value", None)
            try:
                val = float(v) if v is not None and str(v) != "" else None
            except Exception:
                val = None
            return (result != "PASS"), val
    return False, None


def _drop_from_submitable(csv_path, alpha_id):
    """从 submitable_alpha.csv 中删除该 id（兼容 alpha_id / id / alphaId 列名）"""
    if not os.path.exists(csv_path):
        return
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        return
    for col in ("alpha_id", "id", "alphaId"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            new_df = df[df[col] != str(alpha_id)]
            if len(new_df) != len(df):
                new_df.to_csv(csv_path, index=False)
            break


def _append_to_submitted(records_path, alpha_id, out="submitted.csv"):
    """仅把 alpha_id 追加进 submitted.csv（单列），便于下次自动排除"""
    os.makedirs(records_path, exist_ok=True)
    f = os.path.join(records_path, out)
    header_needed = not os.path.exists(f)
    pd.DataFrame([{"alpha_id": str(alpha_id)}]).to_csv(f, index=False, mode="a", header=header_needed)


# ========== 提交主函数 ==========
def submit_alpha(s, alpha_id):
    submit_url = f"https://api.worldquantbrain.com/alphas/{alpha_id}/submit"

    attempts = 0
    while attempts < 5:
        attempts += 1
        print(f"Attempt {attempts} to submit {alpha_id}.")
        # 第一轮提交（POST）
        while True:
            res = s.post(submit_url)
            if res.status_code == 201:
                print(f"Alpha {alpha_id} POST Status 201. Start submitting...")
                break
            elif res.status_code == 400:
                print(f"Alpha {alpha_id} POST Status {res.status_code}.")
                print(f"Alpha {alpha_id} Already POST.")
                print(res.content)
                break
            elif res.status_code == 403:
                print(f"Alpha {alpha_id} POST Status {res.status_code}.")
                try:
                    print(pd.DataFrame(res.json()["is"]["checks"])[['name', 'value', 'result']])
                except Exception:
                    pass
                # 检测 SELF_CORRELATION 失败
                is_fail, sc_val = _parse_self_corr_fail(res)
                if is_fail:
                    print(f"[SELF_CORR_FAIL] {alpha_id} value={sc_val}")
                    return 490  # 自定义返回码：自相关失败
                return res.status_code
            else:
                print(f"Alpha {alpha_id} POST Status {res.status_code}.")
                print(res.content)
                time.sleep(3)

        # 第二轮轮询（GET）
        count = 0
        s_t = datetime.datetime.now()
        while True:
            res = s.get(submit_url)
            if res.status_code == 200:
                retry = res.headers.get('Retry-After', 0)
                if retry:
                    count += 1
                    time.sleep(float(retry))
                    if count % 75 == 0:
                        dur = datetime.datetime.now() - s_t
                        print(f"Alpha {alpha_id} GET Status 200. Waiting... {dur}.")
                else:
                    print(f"Alpha {alpha_id} was submitted successfully.")
                    return res.status_code
            elif res.status_code == 403:
                print(f"Alpha {alpha_id} GET Status {res.status_code}.")
                print(f"Alpha {alpha_id} submit failed. Need Improvement.")
                try:
                    print(pd.DataFrame(res.json()["is"]["checks"])[['name', 'value', 'result']])
                except Exception:
                    pass
                is_fail, sc_val = _parse_self_corr_fail(res)
                if is_fail:
                    print(f"[SELF_CORR_FAIL] {alpha_id} value={sc_val}")
                    return 490
                return res.status_code
            elif res.status_code == 404:
                print(f"Alpha {alpha_id} GET Status {res.status_code}.")
                print(f"Alpha {alpha_id} submit failed. Time Out.")
                break
            else:
                print(f"Alpha {alpha_id} GET Status {res.status_code}.")
                print(f"Alpha {alpha_id} submit failed. Time Out.")
                print(res.headers)
                print(res.content)
                break

    return 404


# ========== 入口 ==========
if __name__ == '__main__':
    s = login()

    submitable_alpha_file = os.path.join(RECORDS_PATH, 'submitable_alpha.csv')

    # 取全部候选（不在这里截断，让提交环节按成功数凑够 N）
    pool = pick_submittable_alpha_ids(
        records_path=RECORDS_PATH,
        ask=False,
        default_n=None,   # 全部
        require_active=False,
    )
    if not pool:
        print("没有候选可提交。")
        raise SystemExit(0)

    # 询问本次目标提交数量 N
    raw = input(f"发现 {len(pool)} 个候选。请输入本次目标提交数量（回车默认 1）：").strip()
    try:
        TARGET_N = max(1, int(raw)) if raw else 1
    except ValueError:
        TARGET_N = 1

    print(f"[queue] 候选 {len(pool)} 个，目标提交 {TARGET_N} 个。")

    success = 0
    for idx, alpha_id in enumerate(pool, 1):
        if success >= TARGET_N:
            break
        print(f"[queue] 尝试提交第 {success+1} 个目标：{alpha_id}（候选序号 {idx}/{len(pool)}）")
        code = submit_alpha(s, alpha_id)

        if code == 200:
            # 成功：从 submitable_alpha.csv 删除，并登记 submitted.csv，计数 +1
            _drop_from_submitable(submitable_alpha_file, alpha_id)
            _append_to_submitted(RECORDS_PATH, alpha_id)
            success += 1

        elif code == 490:
            # SELF_CORRELATION 失败：删除、不计数、继续后面
            print(f"[queue] SELF_CORR FAIL -> 删除 {alpha_id} 并继续尝试后续候选")
            _drop_from_submitable(submitable_alpha_file, alpha_id)

        else:
            # 其它失败：不删除、不计数，继续后面
            print(f"[queue] 跳过 {alpha_id}，status={code}（未计入目标数）")

    print(f"[queue] 完成：成功提交 {success}/{TARGET_N} 个。")
