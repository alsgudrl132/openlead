"""openlead - 신규 개업 리드 수집·분류·발송. 표준 라이브러리만 사용.

사용:
  python openlead.py probe            # API 파라미터·정렬·변경분 규모 확인 (키 필요)
  python openlead.py filesync         # 매일: 구별 인허가 파일 다운로드 → SQLite → 분류 (API 불필요)
  python openlead.py collect          # (예비) API 로 신규 신고분만 수집
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
    kept = fails = 0
    for _ in range(budget):
        try:
            p = fetch_page("info", page, 100)
            fails = 0
        except RuntimeError as e:                        # 트래픽 한도 등 API 거절 → 오늘은 여기까지
            print(f"backfill stopped at page {page}: {e}"); break
        except Exception as e:  # noqa: BLE001  타임아웃·네트워크 → 쉬었다가 같은 페이지 재시도, 연속 6번 실패면 종료
            fails += 1
            print(f"backfill page {page} failed ({fails}): {e}")
            c.commit(); open(state, "w").write(str(page))
            if fails >= 3:                               # 특정 페이지가 서버 쪽에서 계속 타임아웃(124p 사례) → 건너뜀
                print(f"backfill page {page} skipped"); page += 1; fails = 0
                continue
            time.sleep(30 * fails)
            continue
        if not p["items"]:
            print("backfill cycle done at page", page); page = 1; break   # 한 바퀴 끝 → 처음부터 (폐업 갱신용 주기 스캔)
        kept += upsert(c, [i for i in p["items"] if in_region(i, regions)], date.today().isoformat())
        page += 1
        if page % 200 == 0:
            c.commit(); open(state, "w").write(str(page))
    c.commit(); open(state, "w").write(str(page))
    print(f"backfill next_page={page} kept={kept}")



# ---------- 파일 동기화 (API 대신: 인허가 파일을 구 단위로 매일 통째로 받는다) ----------
FILE_HOST = "https://file.localdata.go.kr"
SEOUL_ORG = {  # 개방자치단체코드 → 구 (file.localdata.go.kr 지역 선택 목록, 2026-10-19)
    "3000000": "종로구", "3010000": "중구", "3020000": "용산구", "3030000": "성동구", "3040000": "광진구",
    "3050000": "동대문구", "3060000": "중랑구", "3070000": "성북구", "3080000": "강북구", "3090000": "도봉구",
    "3100000": "노원구", "3110000": "은평구", "3120000": "서대문구", "3130000": "마포구", "3140000": "양천구",
    "3150000": "강서구", "3160000": "구로구", "3170000": "금천구", "3180000": "영등포구", "3190000": "동작구",
    "3200000": "관악구", "3210000": "서초구", "3220000": "강남구", "3230000": "송파구", "3240000": "강동구",
}
KEEP_DAYS = 200   # 영업 중이거나, 최근 200일 내 인허가·폐업인 행만 저장 (명의변경 판정에 충분)


def file_session():
    """세션 쿠키가 있어야 다운로드가 된다. 안내 페이지 → 다운로드 횟수 검증 순서로 한 번 호출."""
    import http.cookiejar
    cj = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    op.addheaders = [("User-Agent", "Mozilla/5.0"), ("Referer", FILE_HOST + "/file/general_restaurants/info")]
    op.open(FILE_HOST + "/file/general_restaurants/info", timeout=60).read()
    op.open(FILE_HOST + "/file/validate/download-count", timeout=60).read()
    return op


def file_rows(op, org_code: str):
    """구 하나의 CSV(cp949)를 받아 API 필드명으로 바꾼 dict 를 낸다."""
    import csv, io
    raw = op.open(f"{FILE_HOST}/file/download/general_restaurants/info?orgCode={org_code}", timeout=600).read()
    rd = csv.reader(io.StringIO(raw.decode("cp949", errors="replace")))
    hdr = next(rd)
    H = {h: i for i, h in enumerate(hdr)}
    g = lambda r, k: (r[H[k]] if k in H and H[k] < len(r) else "").strip()
    for r in rd:
        yield {"MNG_NO": g(r, "관리번호"), "BPLC_NM": g(r, "사업장명"), "BZSTAT_SE_NM": g(r, "업태구분명") or g(r, "위생업태명"),
               "LOTNO_ADDR": g(r, "지번주소"), "ROAD_NM_ADDR": g(r, "도로명주소"), "TELNO": g(r, "전화번호"),
               "LCPMT_YMD": g(r, "인허가일자"), "DTL_SALS_STTS_NM": g(r, "상세영업상태명") or g(r, "영업상태명"),
               "CLSBIZ_YMD": g(r, "폐업일자"), "LAST_MDFCN_PNT": g(r, "최종수정시점")}


def cmd_filesync() -> None:
    """매일: 서울 25개 구 파일을 받아 영업 중 + 최근 200일 인허가/폐업 행을 DB에 반영. API 호출 0."""
    cfg = collect_cfg()
    orgs = cfg.get("org_codes") or list(SEOUL_ORG)
    keep_from = (date.today() - timedelta(days=KEEP_DAYS)).strftime("%Y%m%d")
    today = date.today().isoformat()
    op = file_session()
    c = db()
    total = kept = 0
    for code in orgs:
        batch = []
        for it in file_rows(op, code):
            total += 1
            if (it["DTL_SALS_STTS_NM"].startswith("영업") or ymd(it["CLSBIZ_YMD"]) >= keep_from
                    or ymd(it["LCPMT_YMD"]) >= keep_from):
                batch.append(it)
        kept += upsert(c, batch, today)
        c.commit()
        print(f"{SEOUL_ORG.get(code, code)}: rows={total} kept_total={kept}")
        time.sleep(2)
    reclassify(c, (date.today() - timedelta(days=WEEK_DAYS + 3)).strftime("%Y%m%d"))
    c.commit()
    print(f"filesync done rows={total} kept={kept}")


def fmt_tel(t: str) -> str:
    d = "".join(ch for ch in (t or "") if ch.isdigit())
    if len(d) in (9, 10, 11) and d.startswith("0"):
        if d.startswith("02"):
            return f"02-{d[2:-4]}-{d[-4:]}"
        return f"{d[:3]}-{d[3:-4]}-{d[-4:]}"
    return t or ""


def fmt_line(addr, road, name, biz, d, tel) -> str:
    toks = (addr or "").split()
    dong = toks[2] if len(toks) > 2 else ""
    full = road or addr or ""
    bldg = full.split(",")[0]                           # 상세주소 떼고 건물까지만 → 지도에서 항상 잡힘
    name_link = ("https://alsgudrl132.github.io/openlead/phone.html?"
                 + urllib.parse.urlencode({"q": f"{name} {dong}".strip(), "a": bldg}))
    addr_link = "https://map.naver.com/p/search/" + urllib.parse.quote(bldg)
    return (f'- <a href="{name_link}">{html.escape(name or "")}</a> | {html.escape(biz or "")} | {d[4:6]}/{d[6:]}'
            f' | <a href="{addr_link}">{html.escape(full)}</a>' + (f" | {fmt_tel(tel)}" if tel else "") + "\n")


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
        if not chat or not text or text == "/start":
            continue
        who = (m.get("from") or {})
        label = who.get("username") and "@" + who["username"] or who.get("first_name") or chat
        regions = re.findall(r"[가-힣]+구", text)
        if chat == OWNER:                                # 운영자 명령: 추가 | 연락처 | 구들 | 이름   /  제거 | 연락처
            parts = [p.strip() for p in text.split("|")]
            if parts[0] == "추가" and len(parts) >= 3 and regions:
                add_customer(parts[1], regions, parts[3] if len(parts) > 3 else parts[1], paid=True)
                tg_api("sendMessage", chat_id=OWNER, text=f"추가됨: {parts[1]} / {', '.join(regions)}")
            elif parts[0] == "제거" and len(parts) >= 2:
                remove_customer(parts[1])
                tg_api("sendMessage", chat_id=OWNER, text=f"제거됨: {parts[1]}")
            elif parts[0] == "명단":
                tg_api("sendMessage", chat_id=OWNER, text="\n".join(f"{c['name']} | {c.get('email') or c.get('chat_id')} | {', '.join(c['regions'])}"
                                                               f" | {'유료' if c.get('paid') else '체험 ' + c['ts']}" for c in customers_all()) or "(비어 있음)")
            continue
        if text.startswith("신청") and regions:
            add_customer(chat, regions, label, paid=False, text=text)
            tg_api("sendMessage", chat_id=chat, text=f"접수됐습니다. {', '.join(regions)} 리스트가 다음 월요일 08:00에 옵니다. 2주 무료입니다.")
            tg_api("sendMessage", chat_id=OWNER, text=f"[신청] {label} ({chat})\n{text}")
            n += 1
        else:
            tg_api("sendMessage", chat_id=OWNER, text=f"[문의] {label} ({chat})\n{text}")
    os.makedirs(os.path.dirname(off_path) or ".", exist_ok=True)
    open(off_path, "w").write(str(offset))
    print(f"signups new={n} offset={offset}")


def customers_all() -> list[dict]:
    """signups.jsonl 의 현재 명단. 연락처(이메일 또는 텔레그램 chat_id)당 마지막 줄이 유효, removed 면 제외."""
    if not os.path.exists(SIGNUPS):
        return []
    byid: dict[str, dict] = {}
    for ln in open(SIGNUPS, encoding="utf-8"):
        s = json.loads(ln)
        key = s.get("email") or s.get("chat_id")
        if s.get("removed"):
            byid.pop(key, None)
        else:
            byid[key] = s
    return list(byid.values())


def add_customer(contact: str, regions: list[str], name: str, paid: bool, text: str = "") -> None:
    rec = {"name": name, "regions": regions, "paid": paid, "text": text, "ts": date.today().isoformat()}
    rec["email" if "@" in contact and not contact.startswith("@") else "chat_id"] = contact
    os.makedirs(os.path.dirname(SIGNUPS) or ".", exist_ok=True)
    with open(SIGNUPS, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def remove_customer(contact: str) -> None:
    key = "email" if "@" in contact and not contact.startswith("@") else "chat_id"
    with open(SIGNUPS, "a", encoding="utf-8") as f:
        f.write(json.dumps({key: contact, "removed": True, "ts": date.today().isoformat()}, ensure_ascii=False) + "\n")


def active_customers() -> list[dict]:
    """유료(paid)는 계속, 체험은 TRIAL_DAYS 까지만."""
    out = []
    for s in customers_all():
        if s.get("paid") or (date.today() - date.fromisoformat(s["ts"])).days <= TRIAL_DAYS:
            out.append({"name": s["name"] + ("" if s.get("paid") else " (체험)"), "chat_id": s.get("chat_id"),
                        "email": s.get("email"), "regions": s["regions"], "types": []})
    return out


def email(to: str, subject: str, html_body: str) -> None:
    """Gmail SMTP (앱 비밀번호). GMAIL_USER / GMAIL_APP_PASSWORD 환경변수."""
    import smtplib
    from email.mime.text import MIMEText
    user, pw = os.environ.get("GMAIL_USER", "alsgudrl132@gmail.com"), os.environ["GMAIL_APP_PASSWORD"]
    msg = MIMEText(html_body, "html", "utf-8")
    msg["Subject"], msg["From"], msg["To"] = subject, f"오픈리드 <{user}>", to
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=60) as s:
        s.login(user, pw)
        s.sendmail(user, [to], msg.as_string())


def cmd_send(dry: bool) -> None:
    customers = json.loads(os.environ.get("CUSTOMERS_JSON", "[]")) + active_customers()
    since = (date.today() - timedelta(days=WEEK_DAYS)).strftime("%Y%m%d")
    c = db()
    for cust in customers:
        msg = build_message(c, cust, since)
        to = cust.get("email") or cust.get("chat_id")
        if dry:
            print(f"--- {cust.get('name')} ({to})\n{msg}")
        elif cust.get("email"):
            first = msg.split("\n", 1)[0]
            email(cust["email"], first, "<div style='font:15px/1.6 sans-serif;white-space:pre-wrap'>" + msg + "</div>")
            print("emailed:", cust.get("name"))
        else:
            telegram(cust["chat_id"], msg)
            print("sent:", cust.get("name"))


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")  # Windows cp949 콘솔 대비
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    {"probe": cmd_probe, "collect": cmd_collect, "backfill": cmd_backfill, "signups": cmd_signups, "filesync": cmd_filesync,
     "test-email": lambda: email(os.environ.get("GMAIL_USER", "alsgudrl132@gmail.com"), "[오픈리드] 이메일 발송 테스트",
                                 "<p>이 메일이 보이면 Gmail 발송 설정이 끝난 겁니다. 매주 월요일 08:00 리스트가 이 형식으로 갑니다.</p>"),
     "send": lambda: cmd_send("--dry" in sys.argv)}.get(cmd, lambda: print(__doc__))()
