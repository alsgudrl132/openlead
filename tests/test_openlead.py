"""python -m pytest tests  또는  python tests/test_openlead.py"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ["OPENLEAD_DB"] = os.path.join(tempfile.mkdtemp(), "t.sqlite")

import openlead as ol  # noqa: E402

ITEMS = [
    # 같은 자리, 같은 업태, 폐업 20일 뒤 재인허가 → 명의변경 추정
    {"MNG_NO": "A-old", "BPLC_NM": "옛집", "BZSTAT_SE_NM": "한식", "LOTNO_ADDR": "서울특별시 강남구 역삼동 1",
     "ROAD_NM_ADDR": "서울특별시 강남구 테헤란로 1", "TELNO": "", "LCPMT_YMD": "20200101",
     "DTL_SALS_STTS_NM": "폐업", "CLSBIZ_YMD": "2026-10-01", "LAST_MDFCN_PNT": "20261003"},
    {"MNG_NO": "A-new", "BPLC_NM": "새집", "BZSTAT_SE_NM": "한식", "LOTNO_ADDR": "서울특별시 강남구 역삼동 1",
     "ROAD_NM_ADDR": "서울특별시 강남구 테헤란로 1", "TELNO": "02-1", "LCPMT_YMD": "2026-10-21",
     "DTL_SALS_STTS_NM": "영업/정상", "CLSBIZ_YMD": "", "LAST_MDFCN_PNT": "20261022"},
    # 공실 → 신규
    {"MNG_NO": "B-new", "BPLC_NM": "카페B", "BZSTAT_SE_NM": "커피숍", "LOTNO_ADDR": "서울특별시 서초구 서초동 2",
     "ROAD_NM_ADDR": "서울특별시 서초구 서초대로 2", "TELNO": "", "LCPMT_YMD": "20261020",
     "DTL_SALS_STTS_NM": "영업/정상", "CLSBIZ_YMD": "", "LAST_MDFCN_PNT": "20261021"},
    # 진짜 폐업 (같은 자리에 후속 영업 없음) → 폐업 리드
    {"MNG_NO": "D-closed", "BPLC_NM": "문닫은집", "BZSTAT_SE_NM": "중국식", "LOTNO_ADDR": "서울특별시 강남구 논현동 9",
     "ROAD_NM_ADDR": "서울특별시 강남구 학동로 9", "TELNO": "02-9", "LCPMT_YMD": "20150101",
     "DTL_SALS_STTS_NM": "폐업", "CLSBIZ_YMD": "2026-10-17", "LAST_MDFCN_PNT": "20261018"},
    # 구역 밖 → 제외
    {"MNG_NO": "C-out", "BPLC_NM": "부산집", "BZSTAT_SE_NM": "한식", "LOTNO_ADDR": "부산광역시 해운대구 우동 3",
     "ROAD_NM_ADDR": "", "TELNO": "", "LCPMT_YMD": "20261020",
     "DTL_SALS_STTS_NM": "영업/정상", "CLSBIZ_YMD": "", "LAST_MDFCN_PNT": "20261021"},
]


def test_flow():
    regions = ["강남구", "서초구"]
    hit = [i for i in ITEMS if ol.in_region(i, regions)]
    assert [i["MNG_NO"] for i in hit] == ["A-old", "A-new", "B-new", "D-closed"]

    c = ol.db()
    assert ol.upsert(c, hit, "2026-10-19") == 4
    assert ol.upsert(c, hit, "2026-10-19") == 4          # 재실행해도 중복 없음
    assert c.execute("SELECT COUNT(*) FROM shops").fetchone()[0] == 4

    ol.reclassify(c, "20261015")
    kinds = dict(c.execute("SELECT mgt_no, kind FROM shops WHERE permit_ymd>='20261015'"))
    assert kinds == {"A-new": "명의변경 추정", "B-new": "신규"}

    msg = ol.build_message(c, {"name": "t", "chat_id": "0", "regions": ["강남구"]}, "20261015")
    assert "1건 (전화번호 1건)" in msg and "새집" not in msg and "카페B" not in msg
    assert "폐업 · 전화번호 있음 1건" in msg and "문닫은집" in msg and "옛집" not in msg   # 옛집은 명의변경이라 폐업 리드 아님
    msg2 = ol.build_message(c, {"name": "t", "chat_id": "0", "regions": regions, "types": ["커피숍"], "new": True}, "20261015")
    assert "카페B" in msg2 and "새집" not in msg2


if __name__ == "__main__":
    test_flow()
    print("ok")
