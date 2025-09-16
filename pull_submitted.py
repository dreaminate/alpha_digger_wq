import os
import csv
import time
import argparse
from datetime import datetime, timedelta

from machine_lib import login


def iter_alphas_by_date(session, start_date, end_date, limit=100):
    """
    遍历 status=ACTIVE 的 alphas，按创建时间 [start_date, end_date) 过滤。
    不对 tag 和 type 做任何过滤；仅把返回里的字段原样写出。
    """
    base = "https://api.worldquantbrain.com/users/self/alphas"
    offset = 0
    while True:
        qs = [
            f"limit={limit}",
            f"offset={offset}",
            f"dateCreated%3E={start_date}T00:00:00-04:00",
            f"dateCreated%3C{end_date}T00:00:00-04:00",
            "status=ACTIVE",
            "hidden=false",
            "order=-dateCreated",
        ]
        url = base + "?" + "&".join(qs)
        resp = session.get(url)

        # 限流（兼容大小写）
        ra = resp.headers.get("retry-after") or resp.headers.get("Retry-After")
        if ra:
            time.sleep(float(ra))
            continue

        data = resp.json()
        count = data.get("count", 0)
        results = data.get("results", [])

        for item in results:
            yield item

        offset += limit
        if offset >= count or offset >= 9900:
            break


def ensure_csv(path, header):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, mode='w', newline='', encoding='utf-8') as f:
            csv.DictWriter(f, fieldnames=header).writeheader()


def load_existing_ids(path):
    if not os.path.exists(path):
        return set()
    ids = set()
    try:
        with open(path, mode='r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                if row.get('alpha_id'):
                    ids.add(row['alpha_id'])
    except Exception:
        pass
    return ids


def write_rows(path, rows, header):
    with open(path, mode='a', newline='', encoding='utf-8') as f:
        wr = csv.DictWriter(f, fieldnames=header)
        for r in rows:
            wr.writerow(r)


def backfill_active(start_date, end_date, out_path=os.path.join('records', 'submited.csv')):
    print(f"[Backfill ACTIVE] start={start_date} | end={end_date} | out={out_path}")

    s = login()
    header = [
        # 与界面列一致
        "name", "competition", "type", "status", "dateCreated", "region", "universe",
        "sharpe", "returns", "turnover", "tag", "dateSubmitted",
        # 额外保留
        "timestamp", "alpha_id", "expr", "delay", "decay", "neutralize", "margin", "color", "tags"
    ]
    ensure_csv(out_path, header)
    seen = load_existing_ids(out_path)

    def map_item(it):
        settings = it.get("settings", {}) or {}
        isblk = it.get("is", {}) or {}
        regular = it.get("regular", {}) or {}

        # 取唯一 tag（字符串或单元素列表）→ 直接写入，不做多余判断
        tags_val = it.get("tags")
        tag_single = tags_val if isinstance(tags_val, str) else (tags_val[0] if tags_val else None)

        return {
            "name": it.get("name"),
            "competition": it.get("competition") or it.get("competitionName"),
            "type": it.get("type"),                  # 原样直写
            "status": it.get("status"),
            "dateCreated": it.get("dateCreated"),
            "region": settings.get("region"),
            "universe": settings.get("universe"),
            "sharpe": isblk.get("sharpe"),
            "returns": isblk.get("returns"),
            "turnover": isblk.get("turnover"),
            "tag": tag_single,                       # ← 保存返回对象里的唯一 tag
            "dateSubmitted": it.get("dateSubmitted"),

            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "alpha_id": it.get("id"),
            "expr": regular.get("code"),
            "delay": settings.get("delay"),
            "decay": settings.get("decay"),
            "neutralize": settings.get("neutralization"),
            "margin": isblk.get("margin"),
            "color": it.get("color"),
            "tags": tags_val,                        # 完整 tags 也一并保留（便于排查）
        }

    rows = []
    for rec in iter_alphas_by_date(s, start_date, end_date):
        if rec.get("id") in seen:
            continue
        rows.append(map_item(rec))

    if rows:
        write_rows(out_path, rows, header)
        print(f"Backfilled {len(rows)} ACTIVE rows into {out_path}")
    else:
        print("No new ACTIVE rows to backfill.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill ACTIVE alphas by date into submited.csv (no tag filtering)")
    # 默认开始日期：2025-09-12
    parser.add_argument("--start", type=str, default="2025-09-12",
                        help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", type=str, default=(datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"),
                        help="End date YYYY-MM-DD (exclusive)")
    parser.add_argument("--out", type=str, default=os.path.join("records", "submitted.csv"),
                        help="输出 CSV 路径")
    args = parser.parse_args()

    backfill_active(args.start, args.end, out_path=args.out)
