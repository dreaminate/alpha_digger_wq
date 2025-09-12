import os
import csv
import time
import argparse
from datetime import datetime, timedelta

from machine_lib import login


def iter_alphas_by_tag(session, tag, start_date, end_date, status=None, limit=100):
    """
    Generator that yields alpha records filtered by tag within [start_date, end_date).
    Dates should be strings like '2024-01-01'.
    status: None -> all, or 'UNSUBMITTED' / 'SUBMITTED'.
    """
    base = "https://api.worldquantbrain.com/users/self/alphas"
    offset = 0
    while True:
        qs = [
            f"limit={limit}",
            f"offset={offset}",
            f"tag%3D{tag}",
            f"dateCreated%3E={start_date}T00:00:00-04:00",
            f"dateCreated%3C{end_date}T00:00:00-04:00",
            "type=REGULAR",
            "hidden=false",
            "type!=SUPER",
            "order=-dateCreated",
        ]
        if status:
            qs.append(f"status={status}")
        url = base + "?" + "&".join(qs)

        resp = session.get(url)
        # handle rate limit
        if "retry-after" in resp.headers:
            time.sleep(float(resp.headers["retry-after"]))
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
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()


def load_existing_ids(path):
    if not os.path.exists(path):
        return set()
    ids = set()
    try:
        with open(path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('alpha_id'):
                    ids.add(row['alpha_id'])
    except Exception:
        pass
    return ids


def write_rows(path, rows, header):
    with open(path, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        for row in rows:
            writer.writerow(row)


def backfill(tag, start_date, end_date, include_submitted=False, out_path=os.path.join('records', 'sim_results.csv')):
    s = login()
    header = [
        "timestamp", "tag", "alpha_id", "expr", "region", "universe", "delay", "decay", "neutralize",
        "is_sharpe", "fitness", "turnover", "margin", "dateCreated"
    ]
    ensure_csv(out_path, header)
    seen = load_existing_ids(out_path)

    def map_item(it):
        # Safe map from list endpoint record to row
        settings = it.get('settings', {})
        isblk = it.get('is', {})
        regular = it.get('regular', {})
        return {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'tag': tag,
            'alpha_id': it.get('id'),
            'expr': regular.get('code'),
            'region': settings.get('region'),
            'universe': settings.get('universe'),
            'delay': settings.get('delay'),
            'decay': settings.get('decay'),
            'neutralize': settings.get('neutralization'),
            'is_sharpe': isblk.get('sharpe'),
            'fitness': isblk.get('fitness'),
            'turnover': isblk.get('turnover'),
            'margin': isblk.get('margin'),
            'dateCreated': it.get('dateCreated'),
        }

    rows = []
    # UNSUBMITTED first
    for rec in iter_alphas_by_tag(s, tag, start_date, end_date, status='UNSUBMITTED'):
        if rec.get('id') in seen:
            continue
        rows.append(map_item(rec))
    # optionally SUBMITTED
    if include_submitted:
        for rec in iter_alphas_by_tag(s, tag, start_date, end_date, status='SUBMITTED'):
            if rec.get('id') in seen:
                continue
            rows.append(map_item(rec))

    if rows:
        write_rows(out_path, rows, header)
        print(f"Backfilled {len(rows)} rows into {out_path}")
    else:
        print("No new rows to backfill.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Backfill sim results from BRAIN by tag')
    parser.add_argument('--tag', type=str, default='analyst4_usa_1step', help='Tag/name used when creating alphas')
    parser.add_argument('--start', type=str, default='2024-01-01', help='Start date YYYY-MM-DD')
    parser.add_argument('--end', type=str, default=(datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d'), help='End date YYYY-MM-DD (exclusive)')
    parser.add_argument('--include-submitted', action='store_true', help='Include SUBMITTED status alphas')
    args = parser.parse_args()

    backfill(args.tag, args.start, args.end, include_submitted=args.include_submitted)

