# openlead

지자체 인허가 공공데이터로 "이번 주 우리 구역 신규 개업 가게" 리스트를 만들어 텔레그램으로 보내는 도구.
표준 라이브러리만 사용, GitHub Actions 로 서버 없이 돌아간다. 기획은 `PLAN.md`.

## 로컬 실행
```
set DATA_GO_KR_KEY=...          # 공공데이터포털 15154916 활용신청 후 발급 (Decoding 키)
python openlead.py probe        # API 파라미터·정렬·변경분 규모 확인
python openlead.py collect      # 수집 → data/shops.sqlite
set CUSTOMERS_JSON=[{"name":"me","chat_id":"...","regions":["강남구"]}]
python openlead.py send --dry   # 발송 미리보기
python tests/test_openlead.py   # 분류 규칙 자체 점검
```

## GitHub Secrets
| 이름 | 내용 |
|---|---|
| `DATA_GO_KR_KEY` | 공공데이터포털 인증키 |
| `TELEGRAM_TOKEN` | 봇 토큰 (@BotFather) |
| `CUSTOMERS_JSON` | 고객 목록 JSON (`config.example.json` 참고) |
| `OPENLEAD_COLLECT` | 수집 구역·필터 JSON (`config.example.json` 참고) |

데이터(전화번호 포함)와 고객 정보는 저장소에 커밋하지 않는다. DB 는 Actions cache 에만 있다.
