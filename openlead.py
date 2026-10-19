"""openlead - 신규 개업 리드 수집·분류·발송. 표준 라이브러리만 사용.

사용:
  python openlead.py probe            # API 파라미터·정렬·변경분 규모 확인 (키 필요)
  python openlead.py collect          # 매일: 수집 → SQLite → 분류
  python openlead.py send [--dry]     # 매주: 고객별 리스트 텔레그램 발송

환경변수:
  DATA_GO_KR_KEY   공공데이터포털 인증키 (Decoding 키)
  TELEGRAM_TOKEN   봇 토큰
  CUSTOMERS_JSON   고객 설정 JSON 문자열 (예: config.example.json 의 customers)
  OPENLEAD_DB      SQLite 경로 (기본 data/shops.sqlite)
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta

API = "http://apis.data.go.kr/1741000/general_restaurants"
DB_PATH = os.environ.get("OPENLEAD_DB", "data/shops.sqlite")
TRANSFER_DAYS = 180   # 직전 폐업 ~ 신규 인허가 간격이 이 안이면 명의변경 추정
WEEK_DAYS = 7

SCHEMA = """
CREATE TABLE IF NOT EXISTS shops (
  mgt_no TEXT PRIMARY KEY, name TEXT, biz_type TEXT, addr TEXT, road_addr TEXT, tel TEXT,
  permit_ymd TEXT, status TEXT, close_ymd TEXT, last_mod TEXT, first_seen TEXT, kind TEXT
);
CREATE INDEX IF NOT EXISTS ix_addr ON shops(road_addr, biz_type);
CREATE INDEX IF NOT EXISTS ix_permit ON shops(permit_ymd);
"""


# ---------- API ----------
def fetch_page(op: str, page: int, rows: int = 100, **extra) -> dict:
    key = os.environ["DATA_GO_KR_KEY"].strip()
    if "%" in key:                                   # Encoding 키를 넣어도 동작하게
        key = urllib.parse.unquote(key)
    q = {"serviceKey": key, "type": "json",
         "pageNo": page, "numOfRows": rows, **extra}
    url = f"{API}/{op}?" + urllib.parse.urlencode(q)
    with urllib.request.urlopen(url, timeout=60) as r:
        body = json.loads(r.read().decode("utf-8"))
    if "OpenAPI_ServiceResponse" in body:          # 키·트래픽 오류는 다른 봉투로 옴
        raise RuntimeError(body["OpenAPI_ServiceResponse"]["cmmMsgHeader"])
    resp = body["response"]
    if str(resp["header"]["resultCode"]).lstrip("0") not in ("", "0"):
        raise RuntimeError(resp["header"])
    items = resp["body"].get("items") or {}
    rows_ = items.get("item") if isinstance(items, dict) else items
    if isinstance(rows_, dict):
        rows_ = [rows_]
    return {"total": int(resp["body"].get("totalCount", 0)), "items": rows_ or []}


# ---------- DB ----------
def db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    c = sqlite3.connect(DB_PATH)
    c.executescript(SCHEMA)
    return c


def row_of(item: dict) -> tuple:
    return (item.get("MGT_NO"), item.get("BPLC_NM"), item.get("BSNTPE_NM"),
            item.get("STTUS_ADDR"), item.get("ROAD_NM_ADDR"), item.get("SITE_TEL"),
            item.get("LCPMT_YMD"), item.get("SALS_STTS_NM"), item.get("CLSBIZ_YMD"),
            item.get("LAST_MDFCN_PNT"))


def upsert(c: sqlite3.Connection, items: list[dict], today: str) -> int:
    n = 0
    for it in items:
        if not it.get("MGT_NO"):
            continue
        c.execute("""INSERT INTO shops(mgt_no,name,biz_type,addr,road_addr,tel,permit_ymd,status,close_ymd,last_mod,first_seen)
                     VALUES(?,?,?,?,?,?,?,?,?,?,?)
                     ON CONFLICT(mgt_no) DO UPDATE SET name=excluded.name, biz_type=excluded.biz_type,
                       addr=excluded.addr, road_addr=excluded.road_addr, tel=excluded.tel,
                       permit_ymd=excluded.permit_ymd, status=excluded.status, close_ymd=excluded.close_ymd,
                       last_mod=excluded.last_mod""", row_of(it) + (today,))
        n += 1
    return n


def in_region(item: dict, regions: list[str]) -> bool:
    text = (item.get("STTUS_ADDR") or "") + " " + (item.get("ROAD_NM_ADDR") or "")
    return any(r in text for r in regions)


# ---------- 분류 ----------
def classify(c: sqlite3.Connection, mgt_no: str) -> str:
    """같은 주소·업태에 최근 폐업한 가게가 있으면 명의변경 추정, 아니면 신규."""
    s = c.execute("SELECT road_addr, addr, biz_type, permit_ymd FROM shops WHERE mgt_no=?", (mgt_no,)).fetchone()
    if not s or not s[3]:
        return "신규"
    road, addr, biz, permit = s
    permit_d = datetime.strptime(permit, "%Y%m%d").date()
    prev = c.execute("""SELECT close_ymd FROM shops WHERE mgt_no<>? AND biz_type=? AND close_ymd<>'' AND close_ymd IS NOT NULL
                        AND ((road_addr<>'' AND road_addr=?) OR (addr<>'' AND addr=?))""",
                     (mgt_no, biz, road or "", addr or "")).fetchall()
    for (close,) in prev:
        try:
            gap = (permit_d - datetime.strptime(close, "%Y%m%d").date()).days
        except ValueError:
            continue
        if -30 <= gap <= TRANSFER_DAYS:      # 폐업 직후(또는 서류상 약간 역순)에 같은 자리 같은 업태
            return "명의변경 추정"
    return "신규"


def reclassify(c: sqlite3.Connection, since_ymd: str) -> None:
    ids = [r[0] for r in c.execute("SELECT mgt_no FROM shops WHERE permit_ymd>=?", (since_ymd,))]
    for m in ids:
        c.execute("UPDATE shops SET kind=? WHERE mgt_no=?", (classify(c, m), m))


# ---------- 명령 ----------
def cmd_probe() -> None:
    """키 받은 뒤 1회: 총건수, 1페이지 정렬 상태, 변경 메타 분포를 찍는다."""
    p = fetch_page("info", 1, 100)
    items = p["items"]
    print("totalCount:", p["total"])
    print("fields:", sorted(items[0].keys()) if items else "-")
    mods = [i.get("LAST_MDFCN_PNT") or "" for i in items]
    print("LAST_MDFCN_PNT first/last on page1:", mods[:1], mods[-1:], "sorted desc?", mods == sorted(mods, reverse=True))
    print("DAT_UPDT_SE:", {k: mods and sum(1 for i in items if i.get("DAT_UPDT_SE") == k) for k in ("I", "U", "D")})
    print("permit ymd sample:", [i.get("LCPMT_YMD") for i in items[:5]])
    # 필터 파라미터 후보를 던져보고 totalCount 가 줄어드는지로 존재 여부를 추정
    for name, val in (("lastModTsBgn", (date.today() - timedelta(days=3)).strftime("%Y%m%d")),
                      ("bgnYmd", (date.today() - timedelta(days=3)).strftime("%Y%m%d")),
                      ("opnAtmyGrpCd", "3000000"), ("sidoNm", "서울특별시")):
        try:
            t = fetch_page("info", 1, 1, **{name: val})["total"]
            print(f"  param {name}={val}: totalCount={t}  {'<-- 필터 동작' if t != p['total'] else '(변화 없음)'}")
        except Exception as e:  # noqa: BLE001
            print(f"  param {name}: ERROR {e}")


def cmd_collect() -> None:
    cfg = json.loads(os.environ.get("OPENLEAD_COLLECT", "{}"))
    regions: list[str] = cfg.get("regions") or ["서울특별시"]
    max_pages: int = cfg.get("max_pages", 50)          # ponytail: 정렬 미확인 → 페이지 상한으로 트래픽 보호
    extra: dict = cfg.get("params", {})                # probe 로 찾은 변경분 필터를 여기 넣는다
    cutoff = (date.today() - timedelta(days=WEEK_DAYS + 3)).strftime("%Y%m%d")
    today = date.today().isoformat()
    c = db()
    kept = seen = 0
    for page in range(1, max_pages + 1):
        p = fetch_page("info", page, 100, **extra)
        if not p["items"]:
            break
        seen += len(p["items"])
        hit = [i for i in p["items"] if in_region(i, regions)]
        kept += upsert(c, hit, today)
        mods = [i.get("LAST_MDFCN_PNT") or "" for i in p["items"]]
        if mods and max(mods)[:8] < cutoff:            # 최종수정 내림차순이면 여기서 끊긴다
            break
    reclassify(c, cutoff)
    c.commit()
    print(f"seen={seen} kept={kept} pages<={page}")


def build_message(c: sqlite3.Connection, cust: dict, since_ymd: str) -> str:
    rows = c.execute("""SELECT kind, addr, road_addr, name, biz_type, permit_ymd, tel FROM shops
                        WHERE permit_ymd>=? AND status LIKE '%영업%' ORDER BY kind, permit_ymd DESC""", (since_ymd,)).fetchall()
    rows = [r for r in rows if any(x in (r[1] or "") + (r[2] or "") for x in cust["regions"])]
    if cust.get("types"):
        rows = [r for r in rows if r[4] in cust["types"]]
    head = f"[{since_ymd[4:6]}/{since_ymd[6:]}~ 신규 개업 음식점] {', '.join(cust['regions'])} 총 {len(rows)}건\n"
    out, cur = [], ""
    for kind in ("신규", "명의변경 추정"):
        part = [r for r in rows if (r[0] or "신규") == kind]
        if not part:
            continue
        cur = f"\n■ {kind} {len(part)}건\n"
        for _, addr, road, name, biz, permit, tel in part:
            cur += f"- {name} | {biz} | {permit[4:6]}/{permit[6:]} | {road or addr}" + (f" | {tel}" if tel else "") + "\n"
        out.append(cur)
    return head + "".join(out) if out else head + "(해당 없음)"


def telegram(chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{os.environ['TELEGRAM_TOKEN']}/sendMessage"
    for i in range(0, len(text), 3900):                 # 텔레그램 4096자 제한
        data = urllib.parse.urlencode({"chat_id": chat_id, "text": text[i:i + 3900]}).encode()
        urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=30).read()


def cmd_send(dry: bool) -> None:
    customers = json.loads(os.environ.get("CUSTOMERS_JSON", "[]"))
    since = (date.today() - timedelta(days=WEEK_DAYS)).strftime("%Y%m%d")
    c = db()
    for cust in customers:
        msg = build_message(c, cust, since)
        if dry:
            print(f"--- {cust.get('name')} ({cust.get('chat_id')})\n{msg}")
        else:
            telegram(cust["chat_id"], msg)
            print("sent:", cust.get("name"))


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")  # Windows cp949 콘솔 대비
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    {"probe": cmd_probe, "collect": cmd_collect,
     "send": lambda: cmd_send("--dry" in sys.argv)}.get(cmd, lambda: print(__doc__))()
