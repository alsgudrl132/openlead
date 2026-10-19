"""openlead - 신규 개업 리드 수집·분류·발송. 표준 라이브러리만 사용.

사용:
  python openlead.py probe            # API 파라미터·정렬·변경분 규모 확인 (키 필요)
  python openlead.py collect          # 매일: 수집 → SQLite → 분류
  python openlead.py send [--dry]     # 매주: 고객별 리스트 텔레그램 발송
  python openlead.py backfill         # 1회성: 과거 이력 백필 (BACKFILL_PAGES 로 하루 예산 조절)

환경변수:
  DATA_GO_KR_KEY   공공데이터포털 인증키 (Decoding 키)
  TELEGRAM_TOKEN   봇 토큰
  CUSTOMERS_JSON   고객 설정 JSON 문자열 (예: config.example.json 의 customers)
  OPENLEAD_DB      SQLite 경로 (기본 data/shops.sqlite)
"""
from __future__ import annotations

import html
import json
import os
import sqlite3
import sys
import time
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
    for attempt in range(4):                          # 타임아웃·일시 오류 재시도 (2026-10-19 백필이 타임아웃 1회로 죽음)
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                body = json.loads(r.read().decode("utf-8"))
            break
        except (TimeoutError, OSError, ValueError) as e:
            if attempt == 3:
                raise
            time.sleep(3 * (attempt + 1))
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


def ymd(v: str | None) -> str:
    """'2026-10-16' / '2026-10-16 13:21:52' / '20261016' → '20261016'. 빈값은 ''."""
    return (v or "")[:10].replace("-", "")


def row_of(item: dict) -> tuple:
    # 실제 API 필드명 (2026-10-19 probe 로 확인): MNG_NO, LOTNO_ADDR, TELNO, BZSTAT_SE_NM, DTL_SALS_STTS_NM
    return (item.get("MNG_NO"), item.get("BPLC_NM"),
            item.get("BZSTAT_SE_NM") or item.get("SNTTN_BZSTAT_NM"),
            item.get("LOTNO_ADDR"), item.get("ROAD_NM_ADDR"), item.get("TELNO"),
            ymd(item.get("LCPMT_YMD")), item.get("DTL_SALS_STTS_NM") or item.get("SALS_STTS_NM"),
            ymd(item.get("CLSBIZ_YMD")), item.get("LAST_MDFCN_PNT"))


def upsert(c: sqlite3.Connection, items: list[dict], today: str) -> int:
    n = 0
    for it in items:
        if not it.get("MNG_NO"):
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
    text = (item.get("LOTNO_ADDR") or "") + " " + (item.get("ROAD_NM_ADDR") or "")
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
    """키 받은 뒤: 페이지별 인허가일·수정시각 범위와 변경구분 분포로 정렬 규칙을 추정한다."""
    from collections import Counter
    for page in (1, 2, 3, 5, 10, 20, 50, 100):
        p = fetch_page("info", page, 100)
        it = p["items"]
        if page == 1:
            print("totalCount:", p["total"])
            print("fields:", sorted(it[0].keys()) if it else "-")
        if not it:
            print(f"page {page}: empty"); continue
        permits = sorted(ymd(i.get("LCPMT_YMD")) for i in it)
        mods = sorted((i.get("LAST_MDFCN_PNT") or "") for i in it)
        upd = sorted((i.get("DAT_UPDT_PNT") or "") for i in it)
        print(f"page {page}: permit {permits[0]}~{permits[-1]} | lastmod {mods[0][:16]}~{mods[-1][:16]} | updpnt {upd[0][:16]}~{upd[-1][:16]}"
              f" | SE {dict(Counter(i.get('DAT_UPDT_SE') for i in it))} | status {dict(Counter(i.get('DTL_SALS_STTS_NM') for i in it))}"
              f" | seoul {sum(1 for i in it if in_region(i, ['서울특별시']))}")


NOISE = ("백화점", "코엑스", "마트", "식품관", "푸드코트", "박람회", "축제", "행사", "한시적", "휴게소", "구내식당")


def is_noise(name: str, addr: str) -> bool:
    """방문 영업 리드가 못 되는 곳: 백화점·마트 입점, 행사장 부스, 구내식당 등."""
    text = (name or "") + " " + (addr or "")
    return any(k in text for k in NOISE)


def collect_cfg() -> dict:
    """구역 설정: 저장소의 collect.json 우선(비밀 아님), 없으면 OPENLEAD_COLLECT 환경변수."""
    if os.path.exists("collect.json"):
        return json.load(open("collect.json", encoding="utf-8"))
    return json.loads(os.environ.get("OPENLEAD_COLLECT", "{}"))


def cmd_collect() -> None:
    cfg = collect_cfg()
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
        permits = [ymd(i.get("LCPMT_YMD")) for i in p["items"]]
        if permits and max(permits) < cutoff:           # API 는 인허가일 내림차순 (2026-10-19 probe 확인)
            break
    reclassify(c, cutoff)
    c.commit()
    print(f"seen={seen} kept={kept} pages<={page}")


def cmd_backfill() -> None:
    """과거 이력(폐업 포함)을 구역만 골라 저장. 전체 2.3M건/100 ≈ 23,000페이지, 개발계정 10,000회/일 → 3일.
    OPENLEAD_COLLECT.regions 사용, 진행 위치는 data/backfill.txt 에 저장(캐시)."""
    cfg = collect_cfg()
    regions: list[str] = cfg.get("regions") or ["서울특별시"]
    budget = int(os.environ.get("BACKFILL_PAGES", "9000"))
    state = os.path.join(os.path.dirname(DB_PATH) or ".", "backfill.txt")
    page = int(open(state).read() or 1) if os.path.exists(state) else 1
    c = db()
    kept = 0
    for _ in range(budget):
        try:
            p = fetch_page("info", page, 100)
        except Exception as e:  # noqa: BLE001  트래픽 한도·네트워크 → 진행 위치 저장하고 정상 종료 (캐시 보존)
            print(f"backfill stopped at page {page}: {e}"); break
        if not p["items"]:
            print("backfill cycle done at page", page); page = 1; break   # 한 바퀴 끝 → 처음부터 (폐업 갱신용 주기 스캔)
        kept += upsert(c, [i for i in p["items"] if in_region(i, regions)], date.today().isoformat())
        page += 1
        if page % 200 == 0:
            c.commit(); open(state, "w").write(str(page))
    c.commit(); open(state, "w").write(str(page))
    print(f"backfill next_page={page} kept={kept}")


def fmt_line(addr, road, name, biz, d, tel) -> str:
    toks = (addr or "").split()
    dong = toks[2] if len(toks) > 2 else ""
    full = road or addr or ""
    bldg = full.split(",")[0]                           # 상세주소 떼고 건물까지만 → 지도에서 항상 잡힘
    name_link = ("https://alsgudrl132.github.io/openlead/phone.html?"
                 + urllib.parse.urlencode({"q": f"{name} {dong}".strip(), "a": bldg}))
    addr_link = "https://map.naver.com/p/search/" + urllib.parse.quote(bldg)
    return (f'- <a href="{name_link}">{html.escape(name or "")}</a> | {html.escape(biz or "")} | {d[4:6]}/{d[6:]}'
            f' | <a href="{addr_link}">{html.escape(full)}</a>' + (f" | {tel}" if tel else "") + "\n")


def in_cust_region(cust: dict, addr, road) -> bool:
    return any(x in (addr or "") + (road or "") for x in cust["regions"])


def is_transfer_close(c: sqlite3.Connection, mgt_no: str, addr, road, biz, close_ymd: str) -> bool:
    """폐업했지만 같은 자리·같은 업태로 영업 중인 가게가 있으면 명의변경(가게는 계속) → 폐업 리드 아님."""
    rows = c.execute("""SELECT permit_ymd FROM shops WHERE mgt_no<>? AND biz_type=? AND status LIKE '%영업%'
                        AND ((road_addr<>'' AND road_addr=?) OR (addr<>'' AND addr=?))""",
                     (mgt_no, biz, road or "", addr or "")).fetchall()
    cd = datetime.strptime(close_ymd, "%Y%m%d").date()
    for (permit,) in rows:
        try:
            if abs((datetime.strptime(permit, "%Y%m%d").date() - cd).days) <= TRANSFER_DAYS:
                return True
        except ValueError:
            continue
    return False


def build_message(c: sqlite3.Connection, cust: dict, since_ymd: str) -> str:
    rows = c.execute("""SELECT kind, addr, road_addr, name, biz_type, permit_ymd, tel FROM shops
                        WHERE permit_ymd>=? AND status LIKE '%영업%' ORDER BY kind, permit_ymd DESC""", (since_ymd,)).fetchall()
    rows = [r for r in rows if in_cust_region(cust, r[1], r[2]) and not is_noise(r[3], r[2] or r[1])]
    if cust.get("types"):
        rows = [r for r in rows if r[4] in cust["types"]]
    # 폐업 신고(메인): 전체 스캔(backfill)이 상태를 갱신한 것 중 최근 폐업일. 명의변경은 제외.
    closed = c.execute("""SELECT mgt_no, addr, road_addr, name, biz_type, close_ymd, tel FROM shops
                          WHERE close_ymd>=? AND status LIKE '%폐업%' ORDER BY close_ymd DESC""", (since_ymd,)).fetchall()
    closed = [r for r in closed if in_cust_region(cust, r[1], r[2]) and not is_noise(r[3], r[2] or r[1])
              and not is_transfer_close(c, r[0], r[1], r[2], r[4], r[5])]
    if cust.get("types"):
        closed = [r for r in closed if r[4] in cust["types"]]

    head = (f"[{since_ymd[4:6]}/{since_ymd[6:]}~ 음식점 신고 현황] {', '.join(cust['regions'])}"
            f" · 폐업 {len(closed)}건 · 신규 {len(rows)}건\n")
    out = []
    if closed:
        out.append(f"\n■ 폐업 신고 {len(closed)}건 (상호를 누르면 전화번호)\n" + "".join(fmt_line(*r[1:]) for r in closed))
    for kind in ("신규", "명의변경 추정"):
        part = [r for r in rows if (r[0] or "신규") == kind]
        if part:
            out.append(f"\n■ 신규 개업 · {kind} {len(part)}건\n" + "".join(fmt_line(*r[1:]) for r in part))
    return head + "".join(out) if out else head + "(해당 없음)"


def telegram(chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{os.environ['TELEGRAM_TOKEN']}/sendMessage"
    lines, chunk = text.split("\n"), ""
    chunks = []
    for ln in lines:                                     # 4096자 제한: 줄 단위로 끊어야 <a> 태그가 안 깨짐
        if len(chunk) + len(ln) + 1 > 3800:
            chunks.append(chunk); chunk = ""
        chunk += ln + "\n"
    chunks.append(chunk)
    for part in chunks:
        data = urllib.parse.urlencode({"chat_id": chat_id, "text": part, "parse_mode": "HTML",
                                       "disable_web_page_preview": "true"}).encode()
        urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=30).read()


OWNER = os.environ.get("OWNER_CHAT_ID", "8724562427")
TRIAL_DAYS = 14
SIGNUPS = os.path.join(os.path.dirname(DB_PATH) or ".", "signups.jsonl")


def tg_api(method: str, **params) -> dict:
    url = f"https://api.telegram.org/bot{os.environ['TELEGRAM_TOKEN']}/{method}"
    data = urllib.parse.urlencode(params).encode()
    return json.load(urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=30))


def cmd_signups() -> None:
    """봇에 온 메시지를 읽어 '신청 | 업체 | 구역' 이면 체험 명단에 넣고, 전부 운영자에게 전달한다. 서버 없이 폴링."""
    import re
    off_path = os.path.join(os.path.dirname(DB_PATH) or ".", "tg_offset.txt")
    offset = int(open(off_path).read() or 0) if os.path.exists(off_path) else 0
    res = tg_api("getUpdates", offset=offset, timeout=0)
    n = 0
    for u in res.get("result", []):
        offset = u["update_id"] + 1
        m = u.get("message") or {}
        chat = str((m.get("chat") or {}).get("id") or "")
        text = (m.get("text") or "").strip()
        if not chat or chat == OWNER or not text or text == "/start":
            continue
        who = (m.get("from") or {})
        label = who.get("username") and "@" + who["username"] or who.get("first_name") or chat
        regions = re.findall(r"[가-힣]+구", text)
        if text.startswith("신청") and regions:
            with open(SIGNUPS, "a", encoding="utf-8") as f:
                f.write(json.dumps({"chat_id": chat, "name": label, "regions": regions, "text": text,
                                    "ts": date.today().isoformat()}, ensure_ascii=False) + "\n")
            tg_api("sendMessage", chat_id=chat, text=f"접수됐습니다. {', '.join(regions)} 리스트가 다음 월요일 08:00에 옵니다. 2주 무료입니다.")
            tg_api("sendMessage", chat_id=OWNER, text=f"[신청] {label} ({chat})\n{text}")
            n += 1
        else:
            tg_api("sendMessage", chat_id=OWNER, text=f"[문의] {label} ({chat})\n{text}")
    os.makedirs(os.path.dirname(off_path) or ".", exist_ok=True)
    open(off_path, "w").write(str(offset))
    print(f"signups new={n} offset={offset}")


def trial_customers() -> list[dict]:
    """체험 기간(TRIAL_DAYS) 안인 신청자. 기간이 지나면 CUSTOMERS_JSON 에 옮겨야 계속 받는다."""
    if not os.path.exists(SIGNUPS):
        return []
    out = []
    for ln in open(SIGNUPS, encoding="utf-8"):
        s = json.loads(ln)
        if (date.today() - date.fromisoformat(s["ts"])).days <= TRIAL_DAYS:
            out.append({"name": s["name"] + " (체험)", "chat_id": s["chat_id"], "regions": s["regions"], "types": []})
    return out


def cmd_send(dry: bool) -> None:
    customers = json.loads(os.environ.get("CUSTOMERS_JSON", "[]")) + trial_customers()
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
    {"probe": cmd_probe, "collect": cmd_collect, "backfill": cmd_backfill, "signups": cmd_signups,
     "send": lambda: cmd_send("--dry" in sys.argv)}.get(cmd, lambda: print(__doc__))()
