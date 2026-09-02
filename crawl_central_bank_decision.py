from curl_cffi import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os
import re
import sys
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from crawler_schedule import update_my_schedule

# 대규모 인트 연산 제한 방지용 설정
sys.set_int_max_str_digits(10000)

# 파이어베이스 엔진 초기화
FIREBASE_KEY_PATH = "stockcalender-13042-firebase-adminsdk-fbsvc-18b1748d9a.json"

if os.path.exists(FIREBASE_KEY_PATH):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = FIREBASE_KEY_PATH
    db = firestore.Client(project="stockcalender-13042")
else:
    print(f"⚠️ [경고] 파이어베이스 인증 파일({FIREBASE_KEY_PATH})을 찾을 수 없습니다.")
    print("로컬 드라이런 모드로 계속 진행합니다.")
    db = None

events_ref = db.collection("events") if db else None
logs_ref = db.collection("crawler_logs") if db else None

TASK_NAME = "[crawl_central_bank_decision] 해외 중앙은행(ECB·BOJ) 금리결정 수집"

CATEGORY_NAME = "금리결정"

# 수집 대상: 중앙은행 표의 '(약어)'로 식별 → 저장할 eventName
TARGETS = {
    "(ECB)": "유럽 ECB 중앙은행 금리결정",
    "(BOJ)": "일본 BOJ 중앙은행 금리결정",
}

URL = "https://www.investing.com/central-banks/"

MONTHS = {m: f"{i:02d}" for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}


def _to_iso(date_str):
    """'Sep 10, 2026' -> '2026-09-10' 로 표준화(실패 시 None)"""
    m = re.match(r'([A-Za-z]{3})[a-z]*\s+(\d{1,2}),?\s+(\d{4})', date_str.strip())
    if not m:
        return None
    mon, d, y = m.group(1).title(), m.group(2), m.group(3)
    if mon not in MONTHS:
        return None
    return f"{y}-{MONTHS[mon]}-{int(d):02d}"


def fetch_central_banks():
    """중앙은행 표를 파싱해 [{key약어, bank, rate, nextMeeting, lastChange}] 리스트 반환"""
    print("🌐 1. 인베스팅닷컴 중앙은행 페이지 요청 중...")
    r = requests.get(URL, impersonate="chrome", timeout=20)
    if r.status_code != 200:
        raise Exception(f"페이지 접근 실패 (상태 코드: {r.status_code})")
    print(f"📡 HTTP 응답 성공 (코드: {r.status_code}) | 데이터 길이: {len(r.text)} bytes")

    soup = BeautifulSoup(r.text, "html.parser")
    table = None
    for t in soup.find_all("table"):
        if "Next Meeting" in t.get_text():
            table = t
            break
    if table is None:
        raise Exception("중앙은행 표(Next Meeting)를 찾지 못했습니다. (페이지 구조 변경 가능성)")

    rows = []
    for tr in table.find_all("tr"):
        cells = [re.sub(r'\s+', ' ', c.get_text(" ", strip=True))
                 for c in tr.find_all("td")]
        cells = [c for c in cells if c != ""]
        if len(cells) < 4:
            continue
        rows.append({
            "bank": cells[0],
            "rate": cells[1],
            "nextMeeting": cells[2],
            "lastChange": cells[3],
        })
    return rows


def build_detail(last_change):
    """Last Change 를 '2026년 06월 11일 +25bp 인상 / -25bp 인하' 형식으로 변환.

    부호 없는 bp = 인상(+), '-' 붙은 bp = 인하(-).
    """
    m = re.match(r'(.+?)\s*\(\s*([+-]?)(\d+)\s*bp\s*\)\s*$', last_change.strip(), re.I)
    if not m:
        return f"마지막 금리변동 : {last_change.strip()}"  # 예상치 못한 형식이면 원문 그대로
    date_part, sign, num = m.group(1), m.group(2), m.group(3)
    iso = _to_iso(date_part)
    if iso:
        y, mo, d = iso.split("-")
        kdate = f"{y}년 {mo}월 {d}일"
    else:
        kdate = date_part.strip()
    if sign == "-":
        return f"마지막 금리변동 : {kdate} -{num}bp 인하"
    return f"마지막 금리변동 : {kdate} +{num}bp 인상"


def run_central_bank_crawler():
    now = datetime.now()
    print("\n" + "=" * 60)
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 🚀 해외 중앙은행(ECB·BOJ) 금리결정 수집 크롤러 가동")
    print(f"🎯 대상: {', '.join(TARGETS.values())} | 카테고리: {CATEGORY_NAME}")
    print("=" * 60)

    success_count = 0
    update_count = 0
    skip_count = 0

    try:
        rows = fetch_central_banks()
        print(f"🔎 표에서 총 {len(rows)}개 중앙은행 행을 감지했습니다.\n")

        # 대상 은행만 추출
        picked = []
        for row in rows:
            for token, event_name in TARGETS.items():
                if token in row["bank"]:
                    picked.append((event_name, row))
        if not picked:
            raise Exception("대상 중앙은행(ECB·BOJ)을 표에서 찾지 못했습니다.")

        print("🔥 파이어베이스 Firestore 동기화")
        print("-" * 60)
        for event_name, row in picked:
            db_date_str = _to_iso(row["nextMeeting"])
            if not db_date_str:
                print(f"⚠️  [스킵] {event_name} | Next Meeting 날짜 파싱 실패: {row['nextMeeting']!r}")
                skip_count += 1
                continue
            detail = build_detail(row["lastChange"])
            print(f"📅 {db_date_str} | {event_name} (다음 회의: {row['nextMeeting']})")

            if not events_ref:
                print(f"📝 [드라이런] detail: {detail!r}")
                continue

            existing_docs = events_ref.where(
                filter=FieldFilter("date", "==", db_date_str)
            ).where(
                filter=FieldFilter("category", "==", CATEGORY_NAME)
            ).where(
                filter=FieldFilter("eventName", "==", event_name)
            ).get()

            if len(existing_docs) > 0:
                doc = existing_docs[0]
                if doc.to_dict().get("detail") == detail:
                    print(f"⏭️  [중복 스킵] 이미 최신 상태입니다.")
                    skip_count += 1
                else:
                    doc.reference.update({"detail": detail, "isVerified": True})
                    update_count += 1
                    print(f"🔄  [정보 업데이트] detail 을 갱신했습니다.")
            else:
                events_ref.add({
                    "date": db_date_str,
                    "category": CATEGORY_NAME,
                    "eventName": event_name,
                    "detail": detail,
                    "relatedStocks": "",
                    "url": "",
                    "isVerified": True,
                })
                success_count += 1
                print(f"✅  [신규 삽입] 금리결정 일정을 신규 등록했습니다.")
        print("-" * 60)

        if logs_ref:
            logs_ref.add({
                "timestamp": firestore.SERVER_TIMESTAMP,
                "status": "SUCCESS",
                "task_name": TASK_NAME,
                "added_count": success_count,
                "updated_count": update_count,
                "skipped_count": skip_count,
                "message": f"동기화 종료 - 신규 삽입: {success_count}건, 정보 업데이트: {update_count}건, 중복 스킵: {skip_count}건"
            })

        print("\n" + "=" * 60)
        print(f"🏁 파이프라인 종료. 신규 {success_count}건 / 업데이트 {update_count}건 / 스킵 {skip_count}건")
        print("=" * 60 + "\n")

    except Exception as e:
        error_msg = str(e)
        print(f"\n❌ [에러 발생 및 중단] : {error_msg}")
        if logs_ref:
            logs_ref.add({
                "timestamp": firestore.SERVER_TIMESTAMP,
                "status": "FAILED",
                "task_name": TASK_NAME,
                "added_count": success_count,
                "updated_count": update_count,
                "skipped_count": skip_count,
                "message": f"중앙은행 금리결정 크롤러 실패 에러 로그: {error_msg}"
            })


if __name__ == "__main__":
    # 실행 시 이 배치 자신의 다음 실행 예정시간만 Firestore(crawler_schedules)에 기록
    update_my_schedule(db, __file__)

    # cron('0 0 1 * *')이 UTC·KST 모두 1일이라 별도 날짜 가드 없이 바로 실행
    run_central_bank_crawler()
