"""python -m pytest tests  또는  python tests/test_openlead.py"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ["OPENLEAD_DB"] = os.path.join(tempfile.mkdtemp(), "t.sqlite")

import openlead as ol  # noqa: E402

ITEMS = [
    # 같은 자리, 같은 업태, 폐업 20일 뒤 재인허가 → 명의변경 추정
    {"MGT_NO": "A-old", "BPLC_NM": "옛집", "BSNTPE_NM": "한식", "STTUS_ADDR": "서울특별시 강남구 역삼동 1",
     "ROAD_NM_ADDR": "서울특별시 강남구 테헤란로 1", "SITE_TEL": "", "LCPMT_YMD": "20200101",
     "SALS_STTS_NM": "폐업", "CLSBIZ_YMD": "20261001", "LAST_MDFCN_PNT": "20261003"},
    {"MGT_NO": "A-new", "BPLC_NM": "새집", "BSNTPE_NM": "한식", "STTUS_ADDR": "서울특별시 강남구 역삼동 1",
     "ROAD_NM_ADDR": "서울특별시 강남구 테헤란로 1", "SITE_TEL": "02-1", "LCPMT_YMD": "20261021",
     "SALS_STTS_NM": "영업/정상", "CLSBIZ_YMD": "", "LAST_MDFCN_PNT": "20261022"},
    # 공실 → 신규
    {"MGT_NO": "B-new", "BPLC_NM": "카페B", "BSNTPE_NM": "커피숍", "STTUS_ADDR": "서울특별시 서초구 서초동 2",
     "ROAD_NM_ADDR": "서울특별시 서초구 서초대로 2", "SITE_TEL": "", "LCPMT_YMD": "20261020",
     "SALS_STTS_NM": "영업/정상", "CLSBIZ_YMD": "", "LAST_MDFCN_PNT": "20261021"},
    # 구역 밖 → 제외
    {"MGT_NO": "C-out", "BPLC_NM": "부산집", "BSNTPE_NM": "한식", "STTUS_ADDR": "부산광역시 해운대구 우동 3",
     "ROAD_NM_ADDR": "", "SITE_TEL": "", "LCPMT_YMD": "20261020",
     "SALS_STTS_NM": "영업/정상", "CLSBIZ_YMD": "", "LAST_MDFCN_PNT": "20261021"},
]


def test_flow():
    regions = ["강남구", "서초구"]
    hit = [i for i in ITEMS if ol.in_region(i, regions)]
    assert [i["MGT_NO"] for i in hit] == ["A-old", "A-new", "B-new"]

    c = ol.db()
    assert ol.upsert(c, hit, "2026-10-19") == 3
    assert ol.upsert(c, hit, "2026-10-19") == 3          # 재실행해도 중복 없음
    assert c.execute("SELECT COUNT(*) FROM shops").fetchone()[0] == 3

    ol.reclassify(c, "20261015")
    kinds = dict(c.execute("SELECT mgt_no, kind FROM shops WHERE permit_ymd>='20261015'"))
    assert kinds == {"A-new": "명의변경 추정", "B-new": "신규"}

    msg = ol.build_message(c, {"name": "t", "chat_id": "0", "regions": ["강남구"]}, "20261015")
    assert "총 1건" in msg and "새집" in msg and "카페B" not in msg
    msg2 = ol.build_message(c, {"name": "t", "chat_id": "0", "regions": regions, "types": ["커피숍"]}, "20261015")
    assert "카페B" in msg2 and "새집" not in msg2


if __name__ == "__main__":
    test_flow()
    print("ok")
