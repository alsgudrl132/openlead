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
    prev = c.execute("""SELECT close_ymd FROM shops WHERE mgt_no<>? AND close_ymd<>'' AND close_ymd IS NOT NULL
                        AND ((road_addr<>'' AND road_addr=?) OR (addr<>'' AND addr=?))""",
                     (mgt_no, road or "", addr or "")).fetchall()
    for (close,) in prev:
        try:
            gap = (permit_d - datetime.strptime(close, "%Y%m%d").date()).days
        except ValueError:
            continue
        if -90 <= gap <= TRANSFER_DAYS:      # 옛 가게 폐업이 새 신고 뒤 90일까지 늦게 올 수 있음
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
ORG = {  # 개방자치단체코드 → (시·도, 시·군·구). file.localdata.go.kr 지역 목록, 2026-10-19
    "3000000": ("서울특별시", "종로구"),
    "3010000": ("서울특별시", "중구"),
    "3020000": ("서울특별시", "용산구"),
    "3030000": ("서울특별시", "성동구"),
    "3040000": ("서울특별시", "광진구"),
    "3050000": ("서울특별시", "동대문구"),
    "3060000": ("서울특별시", "중랑구"),
    "3070000": ("서울특별시", "성북구"),
    "3080000": ("서울특별시", "강북구"),
    "3090000": ("서울특별시", "도봉구"),
    "3100000": ("서울특별시", "노원구"),
    "3110000": ("서울특별시", "은평구"),
    "3120000": ("서울특별시", "서대문구"),
    "3130000": ("서울특별시", "마포구"),
    "3140000": ("서울특별시", "양천구"),
    "3150000": ("서울특별시", "강서구"),
    "3160000": ("서울특별시", "구로구"),
    "3170000": ("서울특별시", "금천구"),
    "3180000": ("서울특별시", "영등포구"),
    "3190000": ("서울특별시", "동작구"),
    "3200000": ("서울특별시", "관악구"),
    "3210000": ("서울특별시", "서초구"),
    "3220000": ("서울특별시", "강남구"),
    "3230000": ("서울특별시", "송파구"),
    "3240000": ("서울특별시", "강동구"),
    "3250000": ("부산광역시", "중구"),
    "3260000": ("부산광역시", "서구"),
    "3270000": ("부산광역시", "동구"),
    "3280000": ("부산광역시", "영도구"),
    "3290000": ("부산광역시", "진구"),
    "3300000": ("부산광역시", "동래구"),
    "3310000": ("부산광역시", "남구"),
    "3320000": ("부산광역시", "북구"),
    "3330000": ("부산광역시", "해운대구"),
    "3340000": ("부산광역시", "사하구"),
    "3350000": ("부산광역시", "금정구"),
    "3360000": ("부산광역시", "강서구"),
    "3370000": ("부산광역시", "연제구"),
    "3380000": ("부산광역시", "수영구"),
    "3390000": ("부산광역시", "사상구"),
    "3400000": ("부산광역시", "기장군"),
    "3410000": ("대구광역시", "중구"),
    "3420000": ("대구광역시", "동구"),
    "3430000": ("대구광역시", "서구"),
    "3440000": ("대구광역시", "남구"),
    "3450000": ("대구광역시", "북구"),
    "3460000": ("대구광역시", "수성구"),
    "3470000": ("대구광역시", "달서구"),
    "3480000": ("대구광역시", "달성군"),
    "5141000": ("대구광역시", "군위군"),
    "3491000": ("인천광역시", "영종구"),
    "3501000": ("인천광역시", "제물포구"),
    "3510500": ("인천광역시", "미추홀구"),
    "3520000": ("인천광역시", "연수구"),
    "3530000": ("인천광역시", "남동구"),
    "3540000": ("인천광역시", "부평구"),
    "3550000": ("인천광역시", "계양구"),
    "3561000": ("인천광역시", "서해구"),
    "3570000": ("인천광역시", "강화군"),
    "3580000": ("인천광역시", "옹진군"),
    "3565000": ("인천광역시", "검단구"),
    "5805000": ("전남광주통합특별시", "광주동구"),
    "5810000": ("전남광주통합특별시", "광주서구"),
    "5815000": ("전남광주통합특별시", "광주남구"),
    "5820000": ("전남광주통합특별시", "광주북구"),
    "5825000": ("전남광주통합특별시", "광주광산구"),
    "5780000": ("전남광주통합특별시", "광주목포시"),
    "5785000": ("전남광주통합특별시", "광주여수시"),
    "5790000": ("전남광주통합특별시", "광주순천시"),
    "5795000": ("전남광주통합특별시", "광주나주시"),
    "5800000": ("전남광주통합특별시", "광주광양시"),
    "5830000": ("전남광주통합특별시", "광주담양군"),
    "5835000": ("전남광주통합특별시", "광주곡성군"),
    "5840000": ("전남광주통합특별시", "광주구례군"),
    "5845000": ("전남광주통합특별시", "광주고흥군"),
    "5850000": ("전남광주통합특별시", "광주보성군"),
    "5855000": ("전남광주통합특별시", "광주화순군"),
    "5860000": ("전남광주통합특별시", "광주장흥군"),
    "5865000": ("전남광주통합특별시", "광주강진군"),
    "5870000": ("전남광주통합특별시", "광주해남군"),
    "5875000": ("전남광주통합특별시", "광주영암군"),
    "5880000": ("전남광주통합특별시", "광주무안군"),
    "5885000": ("전남광주통합특별시", "광주함평군"),
    "5890000": ("전남광주통합특별시", "광주영광군"),
    "5895000": ("전남광주통합특별시", "광주장성군"),
    "5900000": ("전남광주통합특별시", "광주완도군"),
    "5905000": ("전남광주통합특별시", "광주진도군"),
    "5910000": ("전남광주통합특별시", "광주신안군"),
    "3640000": ("대전광역시", "동구"),
    "3650000": ("대전광역시", "중구"),
    "3660000": ("대전광역시", "서구"),
    "3670000": ("대전광역시", "유성구"),
    "3680000": ("대전광역시", "대덕구"),
    "3690000": ("울산광역시", "중구"),
    "3700000": ("울산광역시", "남구"),
    "3710000": ("울산광역시", "동구"),
    "3720000": ("울산광역시", "북구"),
    "3730000": ("울산광역시", "울주군"),
    "3910000": ("경기도", "평택시"),
    "3920000": ("경기도", "동두천시"),
    "3930000": ("경기도", "안산시"),
    "3940000": ("경기도", "고양시"),
    "3970000": ("경기도", "과천시"),
    "3980000": ("경기도", "구리시"),
    "3990000": ("경기도", "남양주시"),
    "3740000": ("경기도", "수원시"),
    "3780000": ("경기도", "성남시"),
    "3820000": ("경기도", "의정부시"),
    "3830000": ("경기도", "안양시"),
    "3860000": ("경기도", "부천시"),
    "3900000": ("경기도", "광명시"),
    "4000000": ("경기도", "오산시"),
    "4010000": ("경기도", "시흥시"),
    "4020000": ("경기도", "군포시"),
    "4030000": ("경기도", "의왕시"),
    "4040000": ("경기도", "하남시"),
    "4050000": ("경기도", "용인시"),
    "4060000": ("경기도", "파주시"),
    "4070000": ("경기도", "이천시"),
    "4080000": ("경기도", "안성시"),
    "4090000": ("경기도", "김포시"),
    "5700000": ("경기도", "여주시"),
    "4140000": ("경기도", "연천군"),
    "4160000": ("경기도", "가평군"),
    "4170000": ("경기도", "양평군"),
    "5530000": ("경기도", "화성시"),
    "5540000": ("경기도", "광주시"),
    "5590000": ("경기도", "양주시"),
    "5600000": ("경기도", "포천시"),
    "4181000": ("강원특별자치도", "춘천시"),
    "4191000": ("강원특별자치도", "원주시"),
    "4201000": ("강원특별자치도", "강릉시"),
    "4211000": ("강원특별자치도", "동해시"),
    "4221000": ("강원특별자치도", "태백시"),
    "4231000": ("강원특별자치도", "속초시"),
    "4241000": ("강원특별자치도", "삼척시"),
    "4251000": ("강원특별자치도", "홍천군"),
    "4261000": ("강원특별자치도", "횡성군"),
    "4271000": ("강원특별자치도", "영월군"),
    "4281000": ("강원특별자치도", "평창군"),
    "4291000": ("강원특별자치도", "정선군"),
    "4301000": ("강원특별자치도", "철원군"),
    "4311000": ("강원특별자치도", "화천군"),
    "4321000": ("강원특별자치도", "양구군"),
    "4331000": ("강원특별자치도", "인제군"),
    "4341000": ("강원특별자치도", "고성군"),
    "4351000": ("강원특별자치도", "양양군"),
    "5710000": ("충청북도", "충북청주시"),
    "4390000": ("충청북도", "충북충주시"),
    "4400000": ("충청북도", "충북제천시"),
    "4420000": ("충청북도", "충북보은군"),
    "4430000": ("충청북도", "충북옥천군"),
    "4440000": ("충청북도", "충북영동군"),
    "4450000": ("충청북도", "충북진천군"),
    "4460000": ("충청북도", "충북괴산군"),
    "4470000": ("충청북도", "충북음성군"),
    "4480000": ("충청북도", "충북단양군"),
    "5570000": ("충청북도", "충북증평군"),
    "5680000": ("충청남도", "충남당진시"),
    "4490000": ("충청남도", "충남천안시"),
    "4500000": ("충청남도", "충남공주시"),
    "4510000": ("충청남도", "충남보령시"),
    "4520000": ("충청남도", "충남아산시"),
    "4530000": ("충청남도", "충남서산시"),
    "4540000": ("충청남도", "충남논산시"),
    "4550000": ("충청남도", "충남금산군"),
    "4570000": ("충청남도", "충남부여군"),
    "4580000": ("충청남도", "충남서천군"),
    "4590000": ("충청남도", "충남청양군"),
    "4600000": ("충청남도", "충남홍성군"),
    "4610000": ("충청남도", "충남예산군"),
    "4620000": ("충청남도", "충남태안군"),
    "5580000": ("충청남도", "충남계룡시"),
    "4641000": ("전북특별자치도", "전주시"),
    "4671000": ("전북특별자치도", "군산시"),
    "4681000": ("전북특별자치도", "익산시"),
    "4691000": ("전북특별자치도", "정읍시"),
    "4701000": ("전북특별자치도", "남원시"),
    "4711000": ("전북특별자치도", "김제시"),
    "4721000": ("전북특별자치도", "완주군"),
    "4731000": ("전북특별자치도", "진안군"),
    "4741000": ("전북특별자치도", "무주군"),
    "4751000": ("전북특별자치도", "장수군"),
    "4761000": ("전북특별자치도", "임실군"),
    "4771000": ("전북특별자치도", "순창군"),
    "4781000": ("전북특별자치도", "고창군"),
    "4791000": ("전북특별자치도", "부안군"),
    "5020000": ("경상북도", "경북포항시"),
    "5050000": ("경상북도", "경북경주시"),
    "5060000": ("경상북도", "경북김천시"),
    "5070000": ("경상북도", "경북안동시"),
    "5080000": ("경상북도", "경북구미시"),
    "5090000": ("경상북도", "경북영주시"),
    "5100000": ("경상북도", "경북영천시"),
    "5110000": ("경상북도", "경북상주시"),
    "5120000": ("경상북도", "경북문경시"),
    "5130000": ("경상북도", "경북경산시"),
    "5150000": ("경상북도", "경북의성군"),
    "5160000": ("경상북도", "경북청송군"),
    "5170000": ("경상북도", "경북영양군"),
    "5180000": ("경상북도", "경북영덕군"),
    "5190000": ("경상북도", "경북청도군"),
    "5200000": ("경상북도", "경북고령군"),
    "5210000": ("경상북도", "경북성주군"),
    "5220000": ("경상북도", "경북칠곡군"),
    "5230000": ("경상북도", "경북예천군"),
    "5240000": ("경상북도", "경북봉화군"),
    "5250000": ("경상북도", "경북울진군"),
    "5260000": ("경상북도", "경북울릉군"),
    "5670000": ("경상남도", "경남창원시"),
    "5310000": ("경상남도", "경남진주시"),
    "5330000": ("경상남도", "경남통영시"),
    "5340000": ("경상남도", "경남사천시"),
    "5350000": ("경상남도", "경남김해시"),
    "5360000": ("경상남도", "경남밀양시"),
    "5370000": ("경상남도", "경남거제시"),
    "5380000": ("경상남도", "경남양산시"),
    "5390000": ("경상남도", "경남의령군"),
    "5400000": ("경상남도", "경남함안군"),
    "5410000": ("경상남도", "경남창녕군"),
    "5420000": ("경상남도", "경남고성군"),
    "5430000": ("경상남도", "경남남해군"),
    "5440000": ("경상남도", "경남하동군"),
    "5450000": ("경상남도", "경남산청군"),
    "5460000": ("경상남도", "경남함양군"),
    "5470000": ("경상남도", "경남거창군"),
    "5480000": ("경상남도", "경남합천군"),
    "6510000": ("제주특별자치도", "제주시"),
    "6520000": ("제주특별자치도", "서귀포시"),
}
SEOUL_ORG = {k: v[1] for k, v in ORG.items() if v[0] == "서울특별시"}
KEEP_DAYS = 200   # 영업 중이거나, 최근 200일 내 인허가·폐업인 행만 저장 (명의변경 판정에 충분)


def file_session():
    """세션 쿠키가 있어야 다운로드가 된다. 안내 페이지 → 다운로드 횟수 검증 순서로 한 번 호출."""
    import http.cookiejar
    cj = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    op.addheaders = [("User-Agent", "Mozilla/5.0"), ("Referer", FILE_HOST + "/file/general_restaurants/info")]
    fetch_retry(op, FILE_HOST + "/file/general_restaurants/info", 60)
    fetch_retry(op, FILE_HOST + "/file/validate/download-count", 60)
    return op


def fetch_retry(op, url: str, timeout: int, tries: int = 4) -> bytes:
    """file.localdata.go.kr 가 가끔 응답을 안 준다 → 쉬었다가 재시도 (2026-10-19 첫 접속 타임아웃 사례)."""
    for i in range(tries):
        try:
            return op.open(url, timeout=timeout).read()
        except Exception:  # noqa: BLE001
            if i == tries - 1:
                raise
            time.sleep(30 * (i + 1))
    return b""


def file_rows(op, org_code: str):
    """구 하나의 CSV(cp949)를 받아 API 필드명으로 바꾼 dict 를 낸다."""
    import csv, io
    raw = fetch_retry(op, f"{FILE_HOST}/file/download/general_restaurants/info?orgCode={org_code}", 600)
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
    sidos = cfg.get("sido") or ["서울특별시"]             # 켤 시·도 목록 (collect.json)
    orgs = cfg.get("org_codes") or [k for k, v in ORG.items() if v[0] in sidos]
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
        print(f"{' '.join(ORG.get(code, (code, '')))}: rows={total} kept_total={kept}")
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


def fmt_line(addr, road, name, biz, d, tel, closed: bool = False) -> str:
    toks = (addr or "").split()
    dong = toks[2] if len(toks) > 2 else ""
    full = road or addr or ""
    import re
    bldg = re.sub(r"\s*\([^)]*$", "", full.split(",")[0]).strip()   # 상세주소·잘린 괄호 떼고 건물까지만
    name_link = ("https://alsgudrl132.github.io/openlead/phone.html?"
                 + urllib.parse.urlencode({"q": f"{name} {dong}".strip(), "a": bldg, **({"t": "c"} if closed else {}),
                                           **({"tel": fmt_tel(tel)} if tel else {})}))
    addr_link = "https://map.naver.com/p/search/" + urllib.parse.quote(bldg)
    return (f'- <a href="{name_link}">{html.escape(name or "")}</a> | {html.escape(biz or "")} | {d[4:6]}/{d[6:]}'
            f' | <a href="{addr_link}">{html.escape(full)}</a>' + (f" | {fmt_tel(tel)}" if tel else "") + "\n")


def in_cust_region(cust: dict, addr, road) -> bool:
    return any(x in (addr or "") + (road or "") for x in cust["regions"])


def is_transfer_close(c: sqlite3.Connection, mgt_no: str, addr, road, biz, close_ymd: str) -> bool:
    """폐업했지만 같은 자리·같은 업태로 영업 중인 가게가 있으면 명의변경(가게는 계속) → 폐업 리드 아님."""
    rows = c.execute("""SELECT permit_ymd FROM shops WHERE mgt_no<>? AND status LIKE '%영업%'
                        AND ((road_addr<>'' AND road_addr=?) OR (addr<>'' AND addr=?))""",
                     (mgt_no, road or "", addr or "")).fetchall()          # 업태 무관: 업종 변경·법인 전환도 가게는 계속
    cd = datetime.strptime(close_ymd, "%Y%m%d").date()
    for (permit,) in rows:
        try:
            gap = (datetime.strptime(permit, "%Y%m%d").date() - cd).days
            if -90 <= gap <= TRANSFER_DAYS:              # 새 사장 신고가 폐업 신고보다 먼저 오는 게 보통 (실측 최대 43일 전)
                return True
        except ValueError:
            continue
    return False


_KAKAO_CACHE: dict = {}   # 한 번의 발송 안에서만 재사용 (프로세스 메모리, 디스크 저장 없음)


def _norm_name(s: str) -> str:
    import re
    s = re.sub(r"\([^)]*\)", "", s or "")
    s = re.sub(r"^(주식회사|유한회사|㈜)", "", s)
    return re.sub(r"[\s\-·.,'\"]+", "", s).lower()


def kakao_fill(r):
    """(mgt_no, addr, road, name, biz, ymd, tel). tel 이 비어 있고 KAKAO_REST_KEY 가 있으면
    카카오 키워드 검색에서 상호(정규화 후 완전 동일) + 도로명 건물 주소가 모두 일치하는 결과의 번호만 채운다."""
    import re
    key = os.environ.get("KAKAO_REST_KEY", "").strip()
    mgt_no, addr, road, name, biz, d, tel = r
    if tel or not key or not name:
        return r
    if mgt_no in _KAKAO_CACHE:
        return r[:6] + (_KAKAO_CACHE[mgt_no],)
    bldg = re.sub(r"\s*\([^)]*$", "", (road or "").split(",")[0]).strip()
    target = _norm_name(name)
    found = ""
    toks = (addr or "").split()
    dong = toks[2] if len(toks) > 2 else ""
    for q in (f"{name} {dong}".strip(), name):
        try:
            url = "https://dapi.kakao.com/v2/local/search/keyword.json?" + urllib.parse.urlencode({"query": q, "size": 15})
            req = urllib.request.Request(url, headers={"Authorization": f"KakaoAK {key}"})
            docs = json.load(urllib.request.urlopen(req, timeout=20)).get("documents", [])
        except Exception:  # noqa: BLE001
            break
        for p in docs:
            ra = (p.get("road_address_name") or "").replace("서울 ", "서울특별시 ")
            if _norm_name(p.get("place_name")) == target and bldg and (ra.startswith(bldg) or bldg.startswith(ra) and ra):
                found = p.get("phone") or ""
                break
        if found or docs:
            break
    _KAKAO_CACHE[mgt_no] = found
    return r[:6] + (found,)


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

    def gu(r):                                           # "서울특별시 강남구 …" → 강남구 (구별 묶음용)
        t = ((r[1] or r[2] or "") + " ").split()
        return t[1] if len(t) > 1 else ""

    def section(title, part, closed=False):
        part = sorted(part, key=lambda r: (gu(r), r[5] or ""), reverse=False)
        body, last = "", None
        for r in part:
            g = gu(r)
            if g != last:
                body += f"[{g}]\n"; last = g
            body += fmt_line(*r[1:], closed=closed) if closed else fmt_line(*r[1:])
        return f"\n■ {title} {len(part)}건\n" + body

    closed = [kakao_fill(r) for r in closed]             # 번호 없는 행: 상호·주소가 정확히 일치할 때만 지도 번호 보충 (저장 안 함)
    with_tel = [r for r in closed if (r[6] or "").strip()]
    no_tel = [r for r in closed if not (r[6] or "").strip()]
    want_new = bool(cust.get("new"))                     # 신규 개업은 고객이 원할 때만 (기본 꺼짐)
    head = (f"[{since_ymd[4:6]}/{since_ymd[6:]}~ 폐업 신고 음식점] {', '.join(cust['regions'])}"
            f" · {len(closed)}건 (전화번호 {len(with_tel)}건)" + (f" · 신규 {len(rows)}건" if want_new else "") + "\n")
    out = []
    if with_tel:
        out.append(section("폐업 · 전화번호 있음", with_tel, closed=True))
    if no_tel:
        out.append(section("폐업 · 주소만 (방문·매물용)", no_tel, closed=True)
                   .replace("건\n", "건\n상호를 누르면 지도에서 조회됩니다. 지도에 등록된 가게만 번호가 보이고, 없을 수도 있습니다.\n", 1))
    for kind in (("신규", "명의변경 추정") if want_new else ()):
        part = [r for r in rows if (r[0] or "신규") == kind]
        if part:
            out.append(section(f"신규 개업 · {kind}", part))
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


REGION_RE = r"[가-힣]{1,6}(?:구|시|군)|서울|경기|부산|인천|대구|대전|광주|울산|세종|강원|충북|충남|전북|전남|경북|경남|제주"


def parse_regions(text: str) -> list[str]:
    import re
    seen, out = set(), []
    for r in re.findall(REGION_RE, text):
        if r not in seen:
            seen.add(r); out.append(r)
    return out


def region_has_data(c: sqlite3.Connection, regions: list[str]) -> bool:
    for r in regions:
        if c.execute("SELECT 1 FROM shops WHERE addr LIKE ? OR road_addr LIKE ? LIMIT 1", (f"%{r}%", f"%{r}%")).fetchone():
            return True
    return False


def deliver(cust: dict, msg: str) -> str:
    """고객 한 명에게 메시지 전달. 제목·첫 줄에 수신 업체명(워터마크)."""
    first = msg.split("\n", 1)[0]
    tagged = f"{first} · {cust.get('name','')} 전용" + msg[len(first):]
    if cust.get("email"):
        email(cust["email"], f"{first} · {cust.get('name','')} 전용",
              "<div style='font:15px/1.6 sans-serif;white-space:pre-wrap'>" + tagged + "</div>"
              "<p style='font:12px/1.6 sans-serif;color:#666;margin-top:24px'>오픈리드 · 구청 폐업 신고 기준이며 실제 영업 상태와 다를 수 있습니다 · "
              "<a href='https://alsgudrl132.github.io/openlead/terms.html'>이용약관·환불 규정</a> · 그만 받으시려면 이 메일에 답장</p>")
        return "emailed"
    telegram(cust["chat_id"], tagged)
    return "sent"


def send_now(cust: dict) -> None:
    """추가·신청 직후 이번 주 리스트를 바로 보낸다 (월요일까지 기다리지 않게)."""
    since = (date.today() - timedelta(days=WEEK_DAYS)).strftime("%Y%m%d")
    deliver(cust, build_message(db(), cust, since))


def cmd_signups() -> None:
    """봇에 온 메시지를 읽는다. 운영자: 추가|연락처|구역|이름 / 제거|연락처 / 명단. 신청자: 신청|업체|구역|이메일. 서버 없이 폴링."""
    off_path = os.path.join(os.path.dirname(DB_PATH) or ".", "tg_offset.txt")
    offset = int(open(off_path).read() or 0) if os.path.exists(off_path) else 0
    res = tg_api("getUpdates", offset=offset, timeout=0)
    n = 0
    c = db()
    for u in res.get("result", []):
        offset = u["update_id"] + 1
        m = u.get("message") or {}
        chat = str((m.get("chat") or {}).get("id") or "")
        text = (m.get("text") or "").strip()
        if not chat or not text or text == "/start":
            continue
        who = (m.get("from") or {})
        label = who.get("username") and "@" + who["username"] or who.get("first_name") or chat
        parts = [p.strip() for p in text.split("|")]
        regions = parse_regions(parts[2]) if len(parts) >= 3 else parse_regions(text)   # 구역 칸만 읽어 '도시락집' 오인 방지
        if chat == OWNER:
            if parts[0] == "추가":
                if len(parts) < 3 or not regions:
                    tg_api("sendMessage", chat_id=OWNER, text="형식: 추가 | 이메일(또는 chat_id) | 구역 | 업체명 [| 유료]\n구역 예: 서울 / 강남구, 서초구 / 수원시 / 경기")
                    continue
                name = parts[3] if len(parts) > 3 else parts[1]
                paid = any(x.strip() == "유료" for x in parts[3:])      # 끝에 '유료' 없으면 2주 체험
                if name == "유료":
                    name = parts[1]
                add_customer(parts[1], regions, name, paid=paid)
                warn = "" if region_has_data(c, regions) else "\n주의: 이 구역은 아직 수집하지 않아 리스트가 비어 있습니다. 켜달라고 말씀해 주세요."
                try:
                    send_now({"name": name, "regions": regions, "types": [],
                              "email": parts[1] if "@" in parts[1] and not parts[1].startswith("@") else None,
                              "chat_id": None if "@" in parts[1] else parts[1]})
                    sent = "이번 주 리스트를 지금 보냈습니다."
                except Exception as e:  # noqa: BLE001
                    sent = f"바로 보내기 실패: {e}"
                tg_api("sendMessage", chat_id=OWNER, text=f"추가됨({'유료' if paid else '체험 2주'}): {parts[1]} / {', '.join(regions)} / {name}\n{sent}{warn}")
            elif parts[0] == "제거" and len(parts) >= 2:
                remove_customer(parts[1])
                tg_api("sendMessage", chat_id=OWNER, text=f"제거됨: {parts[1]}")
            elif parts[0] == "명단":
                tg_api("sendMessage", chat_id=OWNER, text="\n".join(
                    f"{x['name']} | {x.get('email') or x.get('chat_id')} | {', '.join(x['regions'])} | {'유료' if x.get('paid') else '체험 ' + x['ts']}"
                    for x in customers_all()) or "(비어 있음)")
            else:
                tg_api("sendMessage", chat_id=OWNER, text="명령: 추가 | 연락처 | 구역 | 업체명  /  제거 | 연락처  /  명단")
            continue
        if parts[0].startswith("신청") and regions:
            biz = parts[1] if len(parts) > 1 else label
            mail = next((p for p in parts if "@" in p and not p.startswith("@")), "")
            if already_tried(biz, mail or chat):
                tg_api("sendMessage", chat_id=chat, text="이미 무료 체험을 받으신 업체입니다. 계속 받으시려면 오픈채팅으로 말씀해 주세요.")
                tg_api("sendMessage", chat_id=OWNER, text=f"[중복 신청 거절] {label} ({chat})\n{text}")
                continue
            contact = mail or chat
            add_customer(contact, regions, biz, paid=False, text=text)
            cust = {"name": biz, "regions": regions, "types": [], "email": mail or None, "chat_id": None if mail else chat}
            warn = "" if region_has_data(c, regions) else " (이 구역은 아직 준비 중이라 운영자가 확인 후 보내드립니다)"
            try:
                send_now(cust); how = "첫 리스트를 지금 보냈습니다."
            except Exception:  # noqa: BLE001
                how = "첫 리스트는 월요일 08:00에 갑니다."
            tg_api("sendMessage", chat_id=chat, text=f"접수됐습니다. {', '.join(regions)} 폐업 리스트, 2주 무료입니다. {how}{warn}")
            tg_api("sendMessage", chat_id=OWNER, text=f"[신청] {label} ({chat})\n{text}")
            n += 1
        else:
            tg_api("sendMessage", chat_id=OWNER, text=f"[문의] {label} ({chat})\n{text}")
    os.makedirs(os.path.dirname(off_path) or ".", exist_ok=True)
    open(off_path, "w").write(str(offset))
    print(f"signups new={n} offset={offset}")


def _norm_biz(s: str) -> str:
    import re
    return re.sub(r"[\s()\-_.㈜주식회사유한회사]", "", s or "").lower()


def already_tried(biz: str, contact: str) -> bool:
    """같은 업체명(정규화) 또는 같은 연락처가 90일 안에 체험·등록된 적 있으면 True."""
    if not os.path.exists(SIGNUPS):
        return False
    nb = _norm_biz(biz)
    for ln in open(SIGNUPS, encoding="utf-8"):
        s = json.loads(ln)
        if s.get("removed"):
            continue
        if (date.today() - date.fromisoformat(s["ts"])).days > 90:
            continue
        if (nb and _norm_biz(s.get("name")) == nb) or contact in (s.get("email"), s.get("chat_id")):
            return True
    return False


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
        if dry:
            print(f"--- {cust.get('name')} ({cust.get('email') or cust.get('chat_id')})\n{msg}")
        else:
            print(deliver(cust, msg) + ":", cust.get("name"))


def cmd_notices() -> None:
    """매일: 체험 종료 2일 전 고객·운영자에게 알림. 매월 1일: 유료 명단 입금 확인 알림."""
    today = date.today()
    for s in customers_all():
        if s.get("paid"):
            continue
        left = TRIAL_DAYS - (today - date.fromisoformat(s["ts"])).days
        if left == 2:
            msg = (f"{s['name']}님, 오픈리드 무료 체험이 이틀 뒤 끝납니다. 계속 받으시려면 이 메시지에 답장 주세요. "
                   f"이후 월 30,000원(계좌이체)입니다. 답이 없으면 자동으로 중단됩니다.")
            try:
                if s.get("email"):
                    email(s["email"], "[오픈리드] 무료 체험이 이틀 뒤 끝납니다", f"<p>{msg}</p>")
                else:
                    telegram(s["chat_id"], msg)
            except Exception as e:  # noqa: BLE001
                print("notice fail", s.get("name"), e)
            tg_api("sendMessage", chat_id=OWNER, text=f"[체험 D-2] {s['name']} | {s.get('email') or s.get('chat_id')} | {', '.join(s['regions'])}\n계속 받겠다고 하면: 추가 | {s.get('email') or s.get('chat_id')} | {', '.join(s['regions'])} | {s['name']} | 유료")
    if today.day == 1:
        paid = [s for s in customers_all() if s.get("paid")]
        body = "\n".join(f"- {s['name']} | {s.get('email') or s.get('chat_id')} | {', '.join(s['regions'])}" for s in paid) or "(유료 고객 없음)"
        tg_api("sendMessage", chat_id=OWNER, text=f"[월 1일 입금 확인] 유료 {len(paid)}명. 미입금은 '제거 | 연락처'.\n{body}")
    print("notices done")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")  # Windows cp949 콘솔 대비
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    {"probe": cmd_probe, "collect": cmd_collect, "backfill": cmd_backfill, "signups": cmd_signups, "filesync": cmd_filesync,
     "send-seoul-test": lambda: email(os.environ.get("GMAIL_USER", "alsgudrl132@gmail.com"), "[테스트] 서울 전체",
                                       "<div style='font:15px/1.6 sans-serif;white-space:pre-wrap'>" + build_message(db(), {"name": "t", "regions": ["서울특별시"]},
                                       (date.today() - timedelta(days=WEEK_DAYS)).strftime("%Y%m%d")) + "</div>"),
     "test-email": lambda: email(os.environ.get("GMAIL_USER", "alsgudrl132@gmail.com"), "[오픈리드] 이메일 발송 테스트",
                                 "<p>이 메일이 보이면 Gmail 발송 설정이 끝난 겁니다. 매주 월요일 08:00 리스트가 이 형식으로 갑니다.</p>"),
     "notices": cmd_notices,
     "send": lambda: cmd_send("--dry" in sys.argv)}.get(cmd, lambda: print(__doc__))()
