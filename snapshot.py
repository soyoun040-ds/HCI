"""
네이버 메이트 인용수 스냅샷 (HCI Team 7)

블로그 정보 API는 '누적 / 이번 달 / 선정 기간' 인용수만 준다.
이번 달 값은 달이 바뀌면 사라지므로, 월말에 찍어둬야 한다.
(예: 9월 인용수 = 10월 스페셜 지원금 선정 기준값)

사용법
    python snapshot.py                    # 메이트 전원 + 스페셜 전원
    python snapshot.py --workers 5

출력
    out/snapshot_YYYYMMDD_HHMM.csv        스냅샷 1회 = 파일 1개
    out/snapshot_latest.csv               가장 최근 스냅샷 (덮어씀)

여러 번 돌려도 서로 덮어쓰지 않으니, 월말에 한 번 더 돌려도 된다.
"""

import csv
import time
import logging
import argparse
import threading
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from crawler import Client, fetch_mates, M_API, OUT

log = logging.getLogger("snapshot")

COLS = [
    "blog_id", "display_name", "topic_id", "topic_name",
    "in_mate_list", "is_special", "special_type",
    "snapshot_at",
    "citation_cumulative",          # 누적 인용수
    "citation_current_month",       # 이번 달 (= 스냅샷의 목적)
    "citation_current_month_no",
    "citation_selection_period",    # 선정 기간(2개월 전) 인용수
    "citation_selection_month_no",
    "special_prev_month_view_count",  # 스페셜 명단이 주는 전월 인용수
    "neighbor_count", "day_visitor_count", "total_visitor_count",
    "is_naver_mate_blog", "is_power_blog", "is_year_of_blog",
    "note",
]


def snap_one(c, mate, now):
    bid = mate["blog_id"]
    info = (c.get(f"{M_API}/blogs/{bid}", referer=f"https://m.blog.naver.com/{bid}") or {}).get("result")
    if not info:
        return {"blog_id": bid, "snapshot_at": now, "note": "블로그 정보 없음(비공개/삭제?)",
                **{k: mate.get(k, "") for k in ("display_name", "topic_id", "topic_name",
                                                "in_mate_list", "is_special", "special_type")}}
    mc = info.get("mateCitations") or {}
    return {
        "blog_id": bid,
        "display_name": mate.get("display_name", ""),
        "topic_id": mate.get("topic_id", ""), "topic_name": mate.get("topic_name", ""),
        "in_mate_list": mate.get("in_mate_list", ""),
        "is_special": mate.get("is_special", ""), "special_type": mate.get("special_type", ""),
        "snapshot_at": now,
        "citation_cumulative": mc.get("cumulativeCount", ""),
        "citation_current_month": mc.get("currentMonthCount", ""),
        "citation_current_month_no": mc.get("currentMonth", ""),
        "citation_selection_period": mc.get("selectionPeriodCount", ""),
        "citation_selection_month_no": mc.get("selectionPeriodMonth", ""),
        "special_prev_month_view_count": mate.get("special_prev_month_view_count", ""),
        "neighbor_count": info.get("subscriberCount", ""),
        "day_visitor_count": info.get("dayVisitorCount", ""),
        "total_visitor_count": info.get("totalVisitorCount", ""),
        "is_naver_mate_blog": info.get("isNaverMateBlog", ""),
        "is_power_blog": info.get("powerBlog", ""),
        "is_year_of_blog": info.get("isYearOfBlog", ""),
        "note": "",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=5)
    args = ap.parse_args()

    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(message)s",
        handlers=[logging.FileHandler(OUT / f"snapshot_{stamp}.log", encoding="utf-8"),
                  logging.StreamHandler()])

    mates = fetch_mates(Client())
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    path = OUT / f"snapshot_{stamp}.csv"

    local = threading.local()
    lock = threading.Lock()
    t0 = time.time()
    rows = []

    def work(m):
        if not hasattr(local, "c"):
            local.c = Client()
        return snap_one(local.c, m, now)

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        w.writeheader()
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(work, m) for m in mates.values()]
            for n, fut in enumerate(as_completed(futs), 1):
                try:
                    row = fut.result()
                except Exception as e:
                    log.error(f"실패: {e}")
                    continue
                with lock:
                    w.writerow(row); f.flush()
                    rows.append(row)
                if n % 100 == 0:
                    el = (time.time() - t0) / n
                    log.info(f"[{n}/{len(mates)}] 남은 예상 {el * (len(mates) - n) / 60:.0f}분")

    ok = [r for r in rows if r.get("citation_current_month") not in ("", None)]
    months = {r["citation_current_month_no"] for r in ok}
    log.info(f"완료 {len(rows)}명 (인용수 확보 {len(ok)}명, 기준 월 {sorted(months)}) → {path}")

    latest = OUT / "snapshot_latest.csv"
    with open(latest, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=COLS); w.writeheader(); w.writerows(rows)
    log.info(f"최근 스냅샷 복사본 → {latest}")


if __name__ == "__main__":
    main()
