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


def clean_text(text):
    if not text:
        return ""
    return re.sub(r'[\s\xa0\xad\t\n\r]+', ' ', text).strip()


def run_ppi_crawler():
    # 실행 시점 기준 올해와 내년을 자동으로 계산합니다.
    current_year_int = datetime.now().year
    current_year = str(current_year_int)
    next_year = str(current_year_int + 1)

    print("\n" + "=" * 60)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 미국 PPI 발표 일정 크롤러 가동")
    print(f"🎯 동적 타겟팅: 올해({current_year}년) 및 내년({next_year}년) 실시간 DB 동기화")
    print("=" * 60)

    url = "https://www.bls.gov/schedule/news_release/ppi.htm"

    months_map = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
        "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }

    all_parsed_events = []
    success_count = 0
    skip_count = 0

    try:
        print("🌐 1. 실제 Chrome 브라우저 세션을 모사하여 BLS 서버로 우회 접근 중...")
        response = requests.get(url, impersonate="chrome", timeout=15)

        if response.status_code != 200:
            raise Exception(f"BLS 페이지 접근 실패 (상태 코드: {response.status_code})")

        print(f"📡 HTTP 응답 성공 (코드: {response.status_code}) | 데이터 길이: {len(response.text)} bytes")

        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table")

        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                row_text = " | ".join([c.get_text().strip() for c in cells])

                if not (current_year in row_text or next_year in row_text):
                    continue

                for cell in cells:
                    cell_clean = cell.get_text().strip()
                    match = re.search(r'([A-Za-z]+)\.?\s+(\d+)\s*,\s*(20\d{2})', cell_clean)
                    if match:
                        month_name = match.group(1)[:3].lower()
                        day_val = int(match.group(2))
                        year_val = match.group(3)

                        if year_val not in [current_year, next_year]:
                            continue

                        month_num = months_map.get(month_name)
                        if not month_num:
                            continue

                        formatted_date = f"{year_val}-{month_num}-{day_val:02d}"
                        if formatted_date not in all_parsed_events:
                            all_parsed_events.append(formatted_date)

        # 백업 본문 텍스트 매칭
        if len(all_parsed_events) == 0:
            print("⚠️ 테이블에서 추출된 일정이 없어, 전역 정규식 본문 매칭으로 전환합니다.")
            body_text = soup.get_text()
            matches = re.finditer(r'([A-Za-z]+)\.?\s+(\d+)\s*,\s*(20\d{2})', body_text)
            for match in matches:
                month_name = match.group(1)[:3].lower()
                day_val = int(match.group(2))
                year_val = match.group(3)

                if year_val not in [current_year, next_year]:
                    continue

                month_num = months_map.get(month_name)
                if not month_num:
                    continue

                formatted_date = f"{year_val}-{month_num}-{day_val:02d}"
                if formatted_date not in all_parsed_events:
                    all_parsed_events.append(formatted_date)

        all_parsed_events = sorted(list(set(all_parsed_events)))

        print(f"\n🔍 2. 동적 수집 완료된 전체 PPI 일정 목록 ({len(all_parsed_events)}건)")
        print("-" * 60)
        if not all_parsed_events:
            print("❌ 조건에 부합하는 수집된 일정이 존재하지 않습니다.")
        else:
            for idx, date_str in enumerate(all_parsed_events, start=1):
                print(f"  [{idx:02d}] 발표일: {date_str} | 미국 생산자물가지수(PPI) 발표")
        print("-" * 60)

        print("\n🔥 3. 파이어베이스 Firestore 데이터베이스 실시간 동기화")
        print("=" * 60)

        category_name = "일반"
        final_event_name = "미국 생산자물가지수(PPI) 발표"

        for db_date_str in all_parsed_events:
            final_detail = ""

            if events_ref:
                existing_docs = events_ref.where(
                    filter=FieldFilter("date", "==", db_date_str)
                ).where(
                    filter=FieldFilter("category", "==", category_name)
                ).where(
                    filter=FieldFilter("eventName", "==", final_event_name)
                ).get()

                if len(existing_docs) > 0:
                    print(f"⏭️  [중복 스킵] 날짜: {db_date_str} | 이미 존재합니다.")
                    skip_count += 1
                else:
                    # 💡 [요구사항 반영] url 필드를 완전히 비워서("") 전송합니다.
                    payload = {
                        "date": db_date_str,
                        "category": category_name,
                        "eventName": final_event_name,
                        "detail": final_detail,
                        "relatedStocks": "",
                        "url": ""
                    }
                    events_ref.add(payload)
                    success_count += 1
                    print(f"✅  [신규 삽입] 날짜: {db_date_str} | 생산자물가지수 일정을 신규 등록했습니다.")
            else:
                print(f"📝 [드라이런] 날짜: {db_date_str} | {final_event_name}")

        # 로그 적재
        if logs_ref:
            log_payload = {
                "timestamp": firestore.SERVER_TIMESTAMP,
                "status": "SUCCESS",
                "task_name": "[crawl_ppi_calendar] 美 생산자물가지수 수집",
                "added_count": success_count,
                "skipped_count": skip_count,
                "message": f"PPI 동기화 종료 - 신규 삽입: {success_count}건, 중복 스킵: {skip_count}건"
            }
            logs_ref.add(log_payload)

        print("\n" + "=" * 60)
        print(f"🏁 파이프라인 프로세스 종료. 추가: {success_count}건 / 스킵: {skip_count}건")
        print("=" * 60 + "\n")

    except Exception as e:
        error_msg = str(e)
        print(f"\n❌ [에러 발생 및 중단] : {error_msg}")
        if logs_ref:
            logs_ref.add({
                "timestamp": firestore.SERVER_TIMESTAMP,
                "status": "FAILED",
                "task_name": "[crawl_ppi_calendar] 美 생산자물가지수 수집",
                "added_count": 0,
                "skipped_count": 0,
                "message": f"PPI 크롤러 실패 에러 로그: {error_msg}"
            })


if __name__ == "__main__":
    # 실행 시 이 배치 자신의 다음 실행 예정시간만 Firestore(crawler_schedules)에 기록
    update_my_schedule(db, __file__)

    # cron('0 0 1 * *')이 UTC·KST 모두 1일이라 별도 날짜 가드 없이 바로 실행
    run_ppi_crawler()
