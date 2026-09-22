"""
네이버 블로그 크롤러 (HCI Team 7 공용) — 네이버 메이트 / 스페셜 지원금 대상
2026-07-31 이전 데이터만 수집

사용법
    # 1) 메이트 전체 목록 + 스페셜 라벨만 받기 (빠름, 1분)
    python crawler.py --collector 소연 --targets-only

    # 2) 분야 지정 크롤링 (스페셜 지원금 대상자는 --with-special 로 전원 포함)
    python crawler.py --collector 소연 --topics 7-12 --with-special

    # 3) blogId 목록 파일로 크롤링 (한 줄에 blogId 하나)
    python crawler.py --collector 소연 --blogs blog_list_소연.txt

    중간에 끊겨도 같은 명령을 다시 실행하면 끝난 블로그는 건너뛰고 이어서 수집한다.

출력
    out/mates_all.csv        메이트 1명 = 1행 (분야, 인용수, 스페셜 여부 = 정답 라벨)
    out/blogs_소연.csv        블로그 1개 = 1행
    out/posts_소연.csv        글 1개 = 1행 (blog_id로 조인)
    out/crawl_소연.log        진행 로그
    out/failed_소연.txt       실패한 blogId (재실행 시 다시 시도됨)

※ 4명이 반드시 이 파일을 그대로 쓸 것. 컬럼 바꾸면 합칠 때 터짐.
   pip install requests beautifulsoup4 lxml
"""

import re
import csv
import json
import time
import random
import logging
import argparse
import threading
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

CUTOFF = datetime(2026, 7, 31, 23, 59, 59)   # 이 날짜 이후 글은 전부 버림
MAX_POSTS_SCAN = 200                          # 블로그당 훑을 최근 글 수 (컷오프 이전)
TOP_N = 50                                    # 그중 좋아요+댓글 상위 N개 본문 수집
RECENT_N = 50                                 # 최근 N건 날짜 기록
MAX_COMMENTS = 100                            # 글당 댓글 원문 최대 수 (컷오프 이전 댓글만)
PAGE_SIZE = 30                                # post-list API 최대값 (50은 400 에러)
WORKERS = 3                                   # 동시에 처리할 블로그 수. 늘리지 말 것.

OUT = Path("out"); OUT.mkdir(exist_ok=True)

M_API = "https://m.blog.naver.com/api"
UA = ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 "
      "(KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1")

TOPICS = {i: f"TOPIC_{i:03d}" for i in range(1, 26)}

# ── 스키마 (절대 수정 금지, 추가만) ──────────────────────────
BLOG_COLS = [
    "blog_id", "blog_name", "assigned_category", "collector", "collected_at",
    "cumulative_citation",      # 메이트 페이지 누적 인용수
    "citation_info_text",       # i버튼 툴팁 내용 (수동 입력 가능)
    "selection_history",        # 네이버메이트/올해의블로그/이달의블로그 등
    "blog_start_date",          # 첫 게시물 날짜
    "total_post_count",
    "neighbor_count",
    "categories_json",          # [{"name":..., "count":...}, ...]
    "note",
    # ── 추가 컬럼 ──
    "topic_id", "topic_name",           # 메이트 분야
    "is_special",                       # 스페셜 지원금 대상 여부 (정답 라벨)
    "special_type",                     # SPECIAL_300 / SPECIAL_1000
    "citation_selection_period",        # 선정 기간(7월) 인용수
    "citation_selection_month",
    "citation_current_month",           # 현재 월 인용수 (수집 시점)
    "day_visitor_count", "total_visitor_count",
    "blog_directory",                   # 네이버 블로그 주제 디렉터리
    "is_power_blog", "is_year_of_blog", "last_year_of_blog",
    "recent50_dates_json",              # 7/31 이전 최근 50건 날짜
    "scanned_post_count",               # 반응 수를 본 글 수 (<= MAX_POSTS_SCAN)
]

POST_COLS = [
    "blog_id", "log_no", "title", "url", "post_date",
    "like_count", "comment_count", "scrap_count",
    "image_count", "char_count",
    "category_name",
    "is_widget_mission",        # '나만의 실천 100일' 등 연재 위젯 여부
    "widget_mission_name",
    "body_text",
    "comments_json",            # [{"author":..., "text":..., "date":...}, ...]
    # ── 추가 컬럼 ──
    "share_count",              # 공개 API의 shareCnt (스크랩 수는 비공개라 scrap_count에도 동일 값)
    "buy_with_own_money",       # 내돈내산 표시 여부
    "video_count", "link_count", "heading_count",
    "post_datetime",
]

WIDGET_PATTERNS = [
    "나만의 실천 100일", "나만의 테마 마스터", "블로그씨", "주간일기",
    "챌린지", "연재", "일일 미션", "오늘일기",
]

log = logging.getLogger("crawler")


# ── 유틸 ────────────────────────────────────────────────────
def sleep():
    """차단 방지. 줄이지 말 것."""
    time.sleep(random.uniform(0.6, 1.2))


class Client:
    """스레드마다 하나. 재시도 + 차단 감지 시 긴 대기."""

    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": UA})
        self.failures = 0       # 재시도 끝에 실패한 요청 수. 0이 아니면 그 블로그는 저장 안 함

    @staticmethod
    def wait_for_network():
        """노트북 덮개 닫힘/와이파이 끊김 → 연결될 때까지 기다림 (시도 횟수로 안 셈)."""
        waited = 0
        while True:
            try:
                requests.head("https://m.blog.naver.com/", timeout=10)
                if waited:
                    log.info(f"네트워크 복구 ({waited}s 대기)")
                return
            except requests.RequestException:
                if waited % 300 == 0:
                    log.warning(f"네트워크 끊김 — 대기 중 ({waited}s)")
                time.sleep(30); waited += 30

    def get(self, url, params=None, referer="https://m.blog.naver.com/", as_json=True):
        attempt = conn_errors = 0
        while attempt < 4 and conn_errors < 8:
            try:
                r = self.s.get(url, params=params, headers={"Referer": referer}, timeout=20)
                if r.status_code in (403, 429) or r.status_code >= 500:
                    wait = 30 * (attempt + 1)
                    log.warning(f"HTTP {r.status_code} {url} → {wait}s 대기")
                    time.sleep(wait)
                    attempt += 1
                    continue
                if r.status_code == 400 or r.status_code == 404:
                    return None
                sleep()
                return r.json() if as_json else r.text
            except (requests.ConnectionError, requests.Timeout) as e:
                conn_errors += 1
                log.warning(f"연결 오류({conn_errors}) {url}: {type(e).__name__}")
                self.wait_for_network()
                time.sleep(5)
            except (requests.RequestException, ValueError) as e:
                attempt += 1
                log.warning(f"요청 실패({attempt}) {url}: {e}")
                time.sleep(5 * attempt)
        self.failures += 1
        return None


def blog_id_from_url(u):
    return (u or "").rstrip("/").split("/")[-1]


def ts_to_dt(ms):
    return datetime.fromtimestamp(ms / 1000) if ms else None


# ── 0. 메이트 목록 + 스페셜 라벨 ─────────────────────────────
MATE_COLS = ["blog_id", "display_name", "topic_id", "topic_name", "expertise_value",
             "cumulative_aib_view_count", "is_new_contributor", "in_mate_list",
             "is_special", "special_type", "special_prev_month_view_count",
             "representative_title", "representative_url", "fetched_at"]


def fetch_mates(c):
    ref = "https://mate.naver.com/"
    mates = {}
    for tid in TOPICS.values():
        js = c.get(f"{M_API}/v1/topic-contributors",
                   {"topicIds": tid, "countPerTopic": 1000}, referer=ref)
        for t in (js or {}).get("result", []):
            for x in t["contributors"]:
                p, rc = x["profile"], x.get("representativeContent") or {}
                bid = blog_id_from_url(p["contributorHomePcUrl"])
                mates[bid] = {
                    "blog_id": bid, "display_name": p.get("displayName", ""),
                    "topic_id": t["topicId"], "topic_name": t["topicName"],
                    "expertise_value": p.get("expertiseValue", ""),
                    "cumulative_aib_view_count": p.get("cumulativeAibViewCount", ""),
                    "is_new_contributor": p.get("isNewContributor", ""),
                    "in_mate_list": True, "is_special": False, "special_type": "",
                    "special_prev_month_view_count": "",
                    "representative_title": rc.get("contentTitle", ""),
                    "representative_url": rc.get("contentPcUrl", ""),
                }

    q = "&".join(f"topicIds={t}" for t in TOPICS.values())
    js = c.get(f"{M_API}/v1/special-reward-contributors?{q}", referer=ref)
    for x in (js or {}).get("result", []):
        p = x["profile"]
        bid = blog_id_from_url(p["contributorHomePcUrl"])
        topic = (p.get("topics") or [{}])[0]
        row = mates.setdefault(bid, {
            "blog_id": bid, "display_name": p.get("displayName", ""),
            "topic_id": topic.get("topicId", ""), "topic_name": topic.get("topicName", ""),
            "expertise_value": "", "cumulative_aib_view_count": "", "is_new_contributor": "",
            "in_mate_list": False, "representative_title": "", "representative_url": "",
        })
        row.update(is_special=True, special_type=p.get("extraRewardType", ""),
                   special_prev_month_view_count=p.get("previousMonthViewCount", ""))

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    with open(OUT / "mates_all.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=MATE_COLS); w.writeheader()
        for r in mates.values():
            w.writerow({**r, "fetched_at": now})
    n_sp = sum(r["is_special"] for r in mates.values())
    log.info(f"메이트 {len(mates)}명 (스페셜 {n_sp}명) → {OUT / 'mates_all.csv'}")
    return mates


# ── 1. 블로그 메타 ──────────────────────────────────────────
def get_blog_meta(c, blog_id):
    ref = f"https://m.blog.naver.com/{blog_id}"
    info = (c.get(f"{M_API}/blogs/{blog_id}", referer=ref) or {}).get("result") or {}
    cats = (c.get(f"{M_API}/blogs/{blog_id}/category-list", referer=ref) or {}).get("result") or {}
    cat_list = [{"name": x.get("categoryName"), "count": x.get("postCnt"),
                 "no": x.get("categoryNo"), "parent_no": x.get("parentCategoryNo")}
                for x in cats.get("mylogCategoryList", []) if not x.get("divisionLine")]
    return info, cat_list


# ── 2. 글 목록 (날짜, 좋아요, 댓글 수 포함) ──────────────────
def post_list_page(c, blog_id, page):
    js = c.get(f"{M_API}/blogs/{blog_id}/post-list",
               {"categoryNo": 0, "itemCount": PAGE_SIZE, "page": page},
               referer=f"https://m.blog.naver.com/{blog_id}")
    return ((js or {}).get("result") or {}).get("items") or []


def get_recent_posts(c, blog_id, max_posts=MAX_POSTS_SCAN):
    """컷오프 이전 최근 글 max_posts개. 목록 자체에 공감/댓글 수가 들어 있다."""
    posts, page = [], 1
    while len(posts) < max_posts:
        items = post_list_page(c, blog_id, page)
        if not items:
            break
        for it in items:
            d = ts_to_dt(it.get("addDate"))
            if not d or d > CUTOFF or it.get("notOpen") or it.get("postBlocked"):
                continue
            posts.append({
                "log_no": str(it["logNo"]), "blog_no": it.get("blogNo"),
                "title": (it.get("titleWithInspectMessage") or "").strip(),
                "post_dt": d,
                "category_name": it.get("categoryName", ""),
                "like_count": it.get("sympathyCnt") or 0,
                "comment_count": it.get("commentCnt") or 0,
                "share_count": it.get("shareCnt") or 0,
                "buy_with_own_money": bool(it.get("buyWithMyOwnMoney")),
            })
        page += 1
    return posts[:max_posts], page


def find_first_post(c, blog_id, known_nonempty=1):
    """마지막 페이지를 이분탐색 → 첫 게시물 날짜와 전체 공개 글 수."""
    lo, hi = known_nonempty, known_nonempty * 2
    while post_list_page(c, blog_id, hi):
        lo, hi = hi, hi * 2
        if hi > 20000:
            break
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if post_list_page(c, blog_id, mid):
            lo = mid
        else:
            hi = mid
    items = post_list_page(c, blog_id, lo)
    dates = [ts_to_dt(it.get("addDate")) for it in items if it.get("addDate")]
    first = min(dates) if dates else None
    total = (lo - 1) * PAGE_SIZE + len(items)
    return first, total


# ── 3. 본문 ─────────────────────────────────────────────────
def get_post_body(c, blog_id, log_no):
    html = c.get("https://m.blog.naver.com/PostView.naver",
                 {"blogId": blog_id, "logNo": log_no},
                 referer=f"https://m.blog.naver.com/{blog_id}", as_json=False)
    empty = {"body_text": "", "image_count": 0, "char_count": 0, "video_count": 0,
             "link_count": 0, "heading_count": 0,
             "is_widget_mission": False, "widget_mission_name": ""}
    if not html:
        return {**empty, "err": "요청 실패"}

    soup = BeautifulSoup(html, "lxml")
    area = (soup.select_one("div.se-main-container")      # 스마트에디터 ONE
            or soup.select_one("div#postViewArea")         # 구 에디터
            or soup.select_one("div.post_ct")
            or soup.select_one("div.post-view"))
    if area is None:
        return {**empty, "err": "본문 없음"}

    text = area.get_text("\n", strip=True)
    imgs = area.select("img.se-image-resource") or [
        i for i in area.select("img") if "sticker" not in " ".join(i.get("class", []))]
    videos = area.select(".se-video, .se-oembed, iframe")
    links = [a for a in area.select("a[href]")
             if a["href"].startswith("http") and "blog.naver.com" not in a["href"]]
    # 스마트에디터는 소제목 대신 인용구 컴포넌트를 쓰는 경우가 많아 둘 다 센다
    headings = area.select(".se-sectionTitle, .se-quotation, h2, h3")

    # 위젯 미션은 본문 밖(글 하단)에 붙는 경우도 있어 페이지 전체 텍스트에서 찾는다
    page_text = soup.get_text(" ", strip=True)
    mission, mname = False, ""
    for pat in WIDGET_PATTERNS:
        if pat in text[-1500:] or pat in text[:300] or pat in page_text[-3000:]:
            mission, mname = True, pat
            break

    return {"body_text": text, "image_count": len(imgs), "char_count": len(text),
            "video_count": len(videos), "link_count": len(links),
            "heading_count": len(headings),
            "is_widget_mission": mission, "widget_mission_name": mname, "err": ""}


# ── 4. 댓글 원문 ────────────────────────────────────────────
def get_comments(c, blog_id, blog_no, log_no, limit=MAX_COMMENTS):
    out, page = [], 1
    while len(out) < limit:
        js = c.get("https://apis.naver.com/commentBox/cbox/web_naver_list_jsonp.json", {
            "ticket": "blog", "templateId": "default", "pool": "blogid", "lang": "ko",
            "objectId": f"{blog_no}_201_{log_no}", "groupId": blog_no,
            "pageSize": 50, "indexSize": 10, "listType": "OBJECT", "pageType": "more",
            "page": page, "initialize": "true", "useAltSort": "true",
        }, referer=f"https://m.blog.naver.com/{blog_id}/{log_no}")
        items = ((js or {}).get("result") or {}).get("commentList") or []
        if not items:
            break
        for x in items:
            d = (x.get("regTime") or "")[:19]
            if x.get("deleted") or (d and datetime.fromisoformat(d) > CUTOFF):
                continue
            out.append({
                "author": x.get("userName") or x.get("maskedUserId") or "",
                "text": re.sub(r"<br\s*/?>", "\n", x.get("contents") or ""),
                "date": x.get("regTime") or x.get("modTime") or "",
                "reply_level": x.get("replyLevel"),
            })
        if len(items) < 50:
            break
        page += 1
    return out[:limit]


# ── 5. 블로그 1개 ───────────────────────────────────────────
def crawl_blog(c, blog_id, mate, collector):
    c.failures = 0
    info, cats = get_blog_meta(c, blog_id)
    if not info:
        raise RuntimeError("블로그 정보 없음 (비공개/삭제?)")

    posts, next_page = get_recent_posts(c, blog_id)
    first_dt, total = find_first_post(c, blog_id, known_nonempty=max(1, next_page - 1))

    top = sorted(posts, key=lambda p: p["like_count"] + p["comment_count"], reverse=True)[:TOP_N]
    rows = []
    for p in top:
        body = get_post_body(c, blog_id, p["log_no"])
        comments = (get_comments(c, blog_id, p["blog_no"], p["log_no"])
                    if p["comment_count"] else [])
        rows.append({
            "blog_id": blog_id, "log_no": p["log_no"], "title": p["title"],
            "url": f"https://blog.naver.com/{blog_id}/{p['log_no']}",
            "post_date": p["post_dt"].strftime("%Y-%m-%d"),
            "post_datetime": p["post_dt"].strftime("%Y-%m-%d %H:%M"),
            "like_count": p["like_count"], "comment_count": p["comment_count"],
            "scrap_count": p["share_count"], "share_count": p["share_count"],
            "buy_with_own_money": p["buy_with_own_money"],
            "image_count": body["image_count"], "char_count": body["char_count"],
            "video_count": body["video_count"], "link_count": body["link_count"],
            "heading_count": body["heading_count"],
            "category_name": p["category_name"],
            "is_widget_mission": body["is_widget_mission"],
            "widget_mission_name": body["widget_mission_name"],
            "body_text": body["body_text"],
            "comments_json": json.dumps(comments, ensure_ascii=False),
        })

    mc = info.get("mateCitations") or {}
    hist = []
    if mate.get("in_mate_list"):
        hist.append("네이버메이트(2026-09)")
    if mate.get("is_special"):
        hist.append(f"스페셜지원금 {mate['special_type']}(2026-09)")
    if info.get("isYearOfBlog") or info.get("lastYearOfBlog"):
        hist.append(f"올해의블로그({info.get('lastYearOfBlog') or ''})")
    if info.get("powerBlog"):
        hist.append("파워블로그")
    citation_text = (f"누적 {mc.get('cumulativeCount', '')} / "
                     f"{mc.get('selectionPeriodMonth', '')}월(선정기간) {mc.get('selectionPeriodCount', '')} / "
                     f"{mc.get('currentMonth', '')}월 {mc.get('currentMonthCount', '')}") if mc else ""

    blog_row = {
        "blog_id": blog_id,
        "blog_name": info.get("blogName", ""),
        "assigned_category": mate.get("topic_name", ""),
        "collector": collector,
        "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "cumulative_citation": mc.get("cumulativeCount", mate.get("cumulative_aib_view_count", "")),
        "citation_info_text": citation_text,
        "selection_history": "; ".join(hist),
        "blog_start_date": first_dt.strftime("%Y-%m-%d") if first_dt else "",
        "total_post_count": total,
        "neighbor_count": info.get("subscriberCount", ""),
        "categories_json": json.dumps(cats, ensure_ascii=False),
        "note": "" if rows else "컷오프 이전 공개 글 없음",
        "topic_id": mate.get("topic_id", ""), "topic_name": mate.get("topic_name", ""),
        "is_special": bool(mate.get("is_special")),
        "special_type": mate.get("special_type", ""),
        "citation_selection_period": mc.get("selectionPeriodCount", ""),
        "citation_selection_month": mc.get("selectionPeriodMonth", ""),
        "citation_current_month": mc.get("currentMonthCount", ""),
        "day_visitor_count": info.get("dayVisitorCount", ""),
        "total_visitor_count": info.get("totalVisitorCount", ""),
        "blog_directory": info.get("blogDirectoryName", ""),
        "is_power_blog": bool(info.get("powerBlog")),
        "is_year_of_blog": bool(info.get("isYearOfBlog")),
        "last_year_of_blog": info.get("lastYearOfBlog", ""),
        "recent50_dates_json": json.dumps(
            [p["post_dt"].strftime("%Y-%m-%d %H:%M") for p in posts[:RECENT_N]]),
        "scanned_post_count": len(posts),
    }
    if c.failures:
        raise RuntimeError(f"요청 {c.failures}건 최종 실패 → 저장 안 함 (재실행 시 다시 수집)")
    return blog_row, rows


# ── 6. 메인 ─────────────────────────────────────────────────
def parse_topics(s):
    out = set()
    for part in s.split(","):
        if "-" in part:
            a, b = part.split("-"); out |= set(range(int(a), int(b) + 1))
        elif part.strip():
            out.add(int(part))
    return {TOPICS[i] for i in out}


def done_ids(path):
    if not path.exists():
        return set()
    with open(path, encoding="utf-8-sig", newline="") as f:
        return {r["blog_id"] for r in csv.DictReader(f)}


def open_writer(path, cols):
    new = not path.exists() or path.stat().st_size == 0
    f = open(path, "a", newline="", encoding="utf-8-sig" if new else "utf-8")
    w = csv.DictWriter(f, fieldnames=cols)
    if new:
        w.writeheader()
    return f, w


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--collector", required=True)
    ap.add_argument("--blogs", help="blogId 목록 txt")
    ap.add_argument("--topics", help="분야 번호, 예: 7-12 또는 1,3,5")
    ap.add_argument("--with-special", action="store_true", help="스페셜 대상자 전원 포함")
    ap.add_argument("--targets-only", action="store_true", help="mates_all.csv만 만들고 종료")
    ap.add_argument("--limit", type=int, default=0, help="앞에서 N개만 (테스트용)")
    ap.add_argument("--workers", type=int, default=WORKERS)
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(threadName)s %(message)s",
        handlers=[logging.FileHandler(OUT / f"crawl_{args.collector}.log", encoding="utf-8"),
                  logging.StreamHandler()])

    mates = fetch_mates(Client())
    if args.targets_only:
        return

    if args.blogs:
        ids = [l.strip() for l in Path(args.blogs).read_text(encoding="utf-8").splitlines()
               if l.strip() and not l.startswith("#")]
    else:
        tset = parse_topics(args.topics) if args.topics else set()
        ids = [b for b, m in mates.items()
               if m["topic_id"] in tset or (args.with_special and m["is_special"])]
    if args.limit:
        ids = ids[:args.limit]

    bpath = OUT / f"blogs_{args.collector}.csv"
    ppath = OUT / f"posts_{args.collector}.csv"
    fpath = OUT / f"failed_{args.collector}.txt"
    done = done_ids(bpath)
    todo = [b for b in ids if b not in done]
    log.info(f"대상 {len(ids)}개 / 완료 {len(ids) - len(todo)}개 / 남음 {len(todo)}개")

    bf, bw = open_writer(bpath, BLOG_COLS)
    pf, pw = open_writer(ppath, POST_COLS)
    lock = threading.Lock()
    local = threading.local()
    failed = []
    t0 = time.time()

    def work(bid):
        if not hasattr(local, "c"):
            local.c = Client()
        return crawl_blog(local.c, bid, mates.get(bid, {}), args.collector)

    try:
        for rnd in range(2):                        # 1차 + 실패분 재시도 1회
            if rnd:
                if not failed:
                    break
                todo, failed = failed, []
                log.info(f"실패 {len(todo)}개 재시도")
            with ThreadPoolExecutor(max_workers=args.workers) as ex:
                futs = {ex.submit(work, b): b for b in todo}
                for n, fut in enumerate(as_completed(futs), 1):
                    bid = futs[fut]
                    try:
                        brow, prows = fut.result()
                        with lock:                  # 글 먼저, 블로그 행은 나중 → 블로그 행이 있으면 완료
                            pw.writerows(prows); pf.flush()
                            bw.writerow(brow); bf.flush()
                        el = (time.time() - t0) / n
                        log.info(f"[{n}/{len(todo)}] ✓ {bid} 글 {len(prows)}건 "
                                 f"(평균 {el:.0f}s/블로그, 남은 예상 {el * (len(todo) - n) / 60:.0f}분)")
                    except Exception as e:
                        failed.append(bid)
                        log.error(f"[{n}/{len(todo)}] ✗ {bid}: {e}")
            t0 = time.time()
    except KeyboardInterrupt:
        log.info("중단 — 다시 실행하면 이어서 수집")
    finally:
        bf.close(); pf.close()
        fpath.write_text("\n".join(failed), encoding="utf-8")

    log.info(f"완료 → {bpath}, {ppath} (실패 {len(failed)}개: {fpath})")


if __name__ == "__main__":
    main()
