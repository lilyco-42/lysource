"""lysource — 资源爬取与整合器

定时抓取配置的可靠信息源 → 清洗去重入库（带可信度评分）→ HTTP API 对外提供
自动化请求服务。单文件核心，SQLite 存储，APScheduler 定时，FastAPI 服务。

启动: uvicorn app:app --host 0.0.0.0 --port 8790
鉴权: 环境变量 LYSOURCE_TOKEN（未设置则所有接口开放，仅限本地开发）
"""
from __future__ import annotations

import hashlib
import html as html_mod
import math
import os
import re
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

import feedparser
import httpx
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from bs4 import BeautifulSoup
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("LYSOURCE_DATA", BASE_DIR / "data"))
DB_PATH = DATA_DIR / "lysource.db"
SOURCES_YAML = Path(os.environ.get("LYSOURCE_SOURCES", BASE_DIR / "sources.yaml"))
TOKEN = os.environ.get("LYSOURCE_TOKEN", "")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
FETCH_TIMEOUT = 15.0
CACHE_TTL = 600  # /v1/fetch 即时抓取缓存秒数

_db_lock = threading.Lock()
_fetch_cache: dict[str, tuple[float, dict]] = {}
_cache_lock = threading.Lock()


# ---------------------------------------------------------------- storage
def db() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    with _db_lock, db() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS sources(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                url TEXT NOT NULL,
                type TEXT NOT NULL DEFAULT 'rss',        -- rss | html
                selector TEXT,                            -- html 类型的 JSON 配置
                trust REAL NOT NULL DEFAULT 0.7,          -- 可信度 0~1
                interval INTEGER NOT NULL DEFAULT 30,     -- 抓取间隔(分钟)
                enabled INTEGER NOT NULL DEFAULT 1,
                last_fetched_at REAL NOT NULL DEFAULT 0,
                last_status TEXT
            );
            CREATE TABLE IF NOT EXISTS items(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL REFERENCES sources(id),
                url TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT DEFAULT '',
                url_hash TEXT UNIQUE NOT NULL,
                fetched_at REAL NOT NULL,
                published_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_items_fetched ON items(fetched_at DESC);
            """
        )


def load_seed_sources() -> int:
    """启动时把 sources.yaml 里的源合并进 DB（按 name 去重，不覆盖已改配置）。"""
    if not SOURCES_YAML.exists():
        return 0
    cfg = yaml.safe_load(SOURCES_YAML.read_text(encoding="utf-8")) or {}
    n = 0
    with _db_lock, db() as c:
        for s in cfg.get("sources", []):
            cur = c.execute(
                "INSERT OR IGNORE INTO sources(name,url,type,selector,trust,interval)"
                " VALUES(?,?,?,?,?,?)",
                (
                    s["name"], s["url"], s.get("type", "rss"),
                    yaml.safe_dump(s.get("selector")) if s.get("selector") else None,
                    float(s.get("trust", 0.7)), int(s.get("interval", 30)),
                ),
            )
            n += cur.rowcount
    return n


# ---------------------------------------------------------------- fetching
def _strip_html(text: str, limit: int = 240) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", html_mod.unescape(text)).strip()[:limit]


def _parse_ts(value) -> float | None:
    """尽力解析 feed published 时间戳。"""
    if not value:
        return None
    for attr in ("published_parsed", "updated_parsed"):
        st = value.get(attr) if isinstance(value, dict) else getattr(value, attr, None)
        if st:
            try:
                return time.mktime(st)
            except Exception:
                pass
    return None


def fetch_source(client: httpx.Client, src: sqlite3.Row) -> tuple[int, str]:
    """抓取单个源并入库，返回 (新增条数, 状态)。"""
    resp = client.get(src["url"], headers={"User-Agent": UA}, follow_redirects=True)
    resp.raise_for_status()
    now = time.time()
    entries: list[tuple[str, str, str, float | None]] = []

    if src["type"] == "rss":
        feed = feedparser.parse(resp.text)
        for e in feed.entries[:50]:
            link = getattr(e, "link", "") or ""
            title = _strip_html(getattr(e, "title", ""), 200)
            if not link or not title:
                continue
            entries.append((link, title, _strip_html(getattr(e, "summary", "")), _parse_ts(e)))
    else:  # html
        sel = yaml.safe_load(src["selector"]) if src["selector"] else {}
        item_sel, title_sel, link_sel = sel.get("item"), sel.get("title"), sel.get("link")
        if not (item_sel and title_sel and link_sel):
            return 0, "error: html 源缺少 selector(item/title/link)"
        soup = BeautifulSoup(resp.text, "html.parser")
        for node in soup.select(item_sel)[:50]:
            a = node.select_one(link_sel)
            t = node.select_one(title_sel)
            if not (a and t):
                continue
            link = a.get("href", "")
            if link.startswith("/"):
                link = str(httpx.URL(src["url"]).join(link))
            title = _strip_html(t.get_text(), 200)
            if link and title:
                entries.append((link, title, "", None))

    added = 0
    with _db_lock, db() as c:
        for link, title, summary, pub in entries:
            h = hashlib.sha1(f"{src['id']}|{link}".encode()).hexdigest()
            cur = c.execute(
                "INSERT OR IGNORE INTO items(source_id,url,title,summary,url_hash,fetched_at,published_at)"
                " VALUES(?,?,?,?,?,?,?)",
                (src["id"], link, title, summary, h, now, pub),
            )
            added += cur.rowcount
        c.execute(
            "UPDATE sources SET last_fetched_at=?, last_status=? WHERE id=?",
            (now, f"ok +{added}", src["id"]),
        )
    return added, f"ok +{added}"


def run_due_sources() -> None:
    """全局心跳：到期的源逐个抓取（单源失败不影响其他源）。"""
    now = time.time()
    with db() as c:
        due = c.execute(
            "SELECT * FROM sources WHERE enabled=1 AND last_fetched_at + interval*60 <= ?",
            (now,),
        ).fetchall()
    if not due:
        return
    with httpx.Client(timeout=FETCH_TIMEOUT) as client:
        for src in due:
            try:
                fetch_source(client, src)
            except Exception as exc:  # 单源失败只记录
                with _db_lock, db() as c:
                    c.execute(
                        "UPDATE sources SET last_fetched_at=?, last_status=? WHERE id=?",
                        (now, f"error: {exc}"[:120], src["id"]),
                    )


# ---------------------------------------------------------------- scoring
def score(trust: float, fetched_at: float, now: float | None = None) -> float:
    """可靠性 = 源可信度 × 时间衰减（72h 半衰期量级）。"""
    now = now or time.time()
    hours = max(0.0, (now - fetched_at) / 3600)
    return round(trust * math.exp(-hours / 72), 4)


# ---------------------------------------------------------------- auth
def require_token(x_api_token: str = Header(default="")) -> None:
    if TOKEN and not hmac.compare_digest(x_api_token, TOKEN):
        raise HTTPException(status_code=401, detail="invalid token")


# ---------------------------------------------------------------- app
@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    seeded = load_seed_sources()
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(run_due_sources, "interval", seconds=60, id="heartbeat",
                  max_instances=1, coalesce=True)
    sched.start()
    run_due_sources()  # 启动立即抓一轮到期源
    print(f"[lysource] started, seeded {seeded} new source(s), db={DB_PATH}")
    yield
    sched.shutdown(wait=False)


app = FastAPI(title="lysource", version="0.1.0",
              description="资源爬取与整合器 · 定时可靠信息源 + 自动化请求服务",
              lifespan=lifespan)


class SourceIn(BaseModel):
    name: str
    url: str
    type: str = "rss"                      # rss | html
    selector: dict | None = None           # html: {item,title,link}
    trust: float = 0.7
    interval: int = 30                     # 分钟


class FetchIn(BaseModel):
    url: str
    selector: str | None = None            # 可选 CSS 选择器，只取匹配文本


@app.get("/healthz")
def healthz():
    return {"ok": True, "time": time.time()}


@app.get("/v1/sources", dependencies=[Depends(require_token)])
def list_sources():
    with db() as c:
        rows = c.execute("SELECT * FROM sources ORDER BY id").fetchall()
    return {"sources": [dict(r) for r in rows]}


@app.post("/v1/sources", dependencies=[Depends(require_token)])
def add_source(body: SourceIn):
    if body.type not in ("rss", "html"):
        raise HTTPException(400, "type 只支持 rss | html")
    if body.type == "html" and not body.selector:
        raise HTTPException(400, "html 源必须提供 selector{item,title,link}")
    with _db_lock, db() as c:
        cur = c.execute(
            "INSERT OR IGNORE INTO sources(name,url,type,selector,trust,interval)"
            " VALUES(?,?,?,?,?,?)",
            (body.name, body.url, body.type,
             yaml.safe_dump(body.selector) if body.selector else None,
             body.trust, body.interval),
        )
        if cur.rowcount == 0:
            raise HTTPException(409, f"source '{body.name}' 已存在")
        sid = cur.lastrowid
    run_due_sources()  # 新源立即抓
    return {"ok": True, "id": sid}


@app.get("/v1/resources", dependencies=[Depends(require_token)])
def resources(q: str = "", limit: int = 50):
    """整合资源查询：关键词过滤（可选）+ 可靠性评分排序。"""
    limit = max(1, min(limit, 200))
    with db() as c:
        rows = c.execute(
            """SELECT i.title, i.url, i.summary, i.fetched_at, i.published_at,
                      s.name AS source, s.trust, s.url AS source_url
               FROM items i JOIN sources s ON s.id = i.source_id
               ORDER BY i.fetched_at DESC LIMIT ?""",
            (limit * 5,),
        ).fetchall()
    now = time.time()
    items = []
    for r in rows:
        if q and q.lower() not in r["title"].lower() and q.lower() not in r["summary"].lower():
            continue
        items.append({
            "title": r["title"], "url": r["url"], "summary": r["summary"],
            "source": r["source"], "source_url": r["source_url"],
            "fetched_at": r["fetched_at"], "published_at": r["published_at"],
            "score": score(r["trust"], r["fetched_at"], now),
        })
        if len(items) >= limit:
            break
    items.sort(key=lambda x: -x["score"])
    return {"count": len(items), "items": items}


@app.post("/v1/fetch", dependencies=[Depends(require_token)])
def fetch_now(body: FetchIn):
    """自动化请求服务：即时代理抓取任意 URL（带 10 分钟缓存）。"""
    now = time.time()
    with _cache_lock:
        hit = _fetch_cache.get(body.url)
        if hit and now - hit[0] < CACHE_TTL:
            return {**hit[1], "cached": True}
    try:
        with httpx.Client(timeout=FETCH_TIMEOUT) as client:
            resp = client.get(body.url, headers={"User-Agent": UA}, follow_redirects=True)
            resp.raise_for_status()
    except Exception as exc:
        raise HTTPException(502, f"fetch failed: {exc}")
    text = resp.text
    if body.selector:
        soup = BeautifulSoup(text, "html.parser")
        text = "\n".join(n.get_text(strip=True) for n in soup.select(body.selector)[:100])
    else:
        text = _strip_html(text, 8000)
    result = {"ok": True, "url": str(resp.url), "status": resp.status_code,
              "length": len(text), "content": text}
    with _cache_lock:
        _fetch_cache[body.url] = (now, result)
    return result


@app.post("/v1/refresh", dependencies=[Depends(require_token)])
def refresh():
    """手动触发全量抓取（阻塞直到完成）。"""
    run_due_sources()
    return {"ok": True}
