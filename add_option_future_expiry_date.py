import calendar
from datetime import datetime
import os
import sys

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

# 정수 변환 제한 확장
sys.set_int_max_str_digits(10000)

# 파이어베이스 엔진 초기화
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "stockcalender-13042-firebase-adminsdk-fbsvc-18b1748d9a.json"
db = firestore.Client()
events_ref = db.collection("events")
logs_ref = db.collection("crawler_logs")

CATEGORY_NAME = "주의"

QUAD_WITCHING_MONTHS = {3, 6, 9, 12}

QUAD_WITCHING_EVENT_NAME = "국내 선옵 동시만기일(3, 6, 9, 12월의 2번째 목요일)"
QUAD_WITCHING_DETAIL = (
    "만기가 되는 대표적인 상품은 '코스피200 선물/옵션', '코스닥150 선물/옵션'"
)

OPTION_EXPIRY_EVENT_NAME = "국내 옵션만기일(매월 2번째 목요일)"
OPTION_EXPIRY_DETAIL = (
    "만기가 되는 대표적인 상품은 '코스피200 옵션', '코스닥150 옵션'\n"
    "추가적으로 '미니 코스피200선물/옵션' 또한 만기(미니는 월마다 만기도래)\n"
    "옵션은 콜옵션(살수있는 권리), 풋옵션(팔수있는 권리)을 의미하며, "
    "만기일 이를 행하지 않으면 권한이 소멸됨."
)


def get_second_thursday(year: int, month: int) -> int:
    # calendar.THURSDAY == 3. monthcalendar()의 각 주(week)에서 목요일 칸만 모아 2번째 값을 취함.
    weeks = calendar.monthcalendar(year, month)
    thursdays = [week[calendar.THURSDAY] for week in weeks if week[calendar.THURSDAY] != 0]
    return thursdays[1]


def build_target_months(base_date: datetime):
    this_year, this_month = base_date.year, base_date.month
    if this_month == 12:
        next_year, next_month = this_year + 1, 1
    else:
        next_year, next_month = this_year, this_month + 1
    return [(this_year, this_month), (next_year, next_month)]


def run_option_expiry_crawler():
    now = datetime.now()

    print("\n" + "=" * 60)
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 🚀 국내 옵션만기일(매월 2번째 목요일) 일정 생성기 가동")
    print("=" * 60)

    success_count = 0
    update_count = 0
    skip_count = 0

    try:
        target_months = build_target_months(now)

        for year, month in target_months:
            day = get_second_thursday(year, month)
            db_date_str = f"{year}-{month:02d}-{day:02d}"

            if month in QUAD_WITCHING_MONTHS:
                event_name = QUAD_WITCHING_EVENT_NAME
                detail = QUAD_WITCHING_DETAIL
            else:
                event_name = OPTION_EXPIRY_EVENT_NAME
                detail = OPTION_EXPIRY_DETAIL

            print(f"📅 대상 일정: {db_date_str} | {event_name}")

            existing_docs = events_ref.where(
                filter=FieldFilter("date", "==", db_date_str)
            ).where(
                filter=FieldFilter("category", "==", CATEGORY_NAME)
            ).where(
                filter=FieldFilter("eventName", "==", event_name)
            ).get()

            if len(existing_docs) > 0:
                doc = existing_docs[0]
                existing_data = doc.to_dict()

                if detail == existing_data.get("detail"):
                    print(f"⏭️  [중복 스킵] 날짜: {db_date_str} | 이미 존재합니다.")
                    skip_count += 1
                    continue

                doc.reference.update({
                    "detail": detail,
                    "url": ""
                })
                update_count += 1
                print(f"🔄  [정보 업데이트] 날짜: {db_date_str} | 세부 내용을 갱신했습니다.")
            else:
                payload = {
                    "date": db_date_str,
                    "category": CATEGORY_NAME,
                    "eventName": event_name,
                    "detail": detail,
                    "relatedStocks": "",
                    "url": ""
                }
                events_ref.add(payload)
                success_count += 1
                print(f"✅  [신규 삽입] 날짜: {db_date_str} | 일정을 신규 등록했습니다.")

        log_payload = {
            "timestamp": firestore.SERVER_TIMESTAMP,
            "status": "SUCCESS",
            "task_name": "[crawl_option_expiry_calendar] 국내 옵션만기일 일정 생성",
            "added_count": success_count,
            "updated_count": update_count,
            "skipped_count": skip_count,
            "message": f"동기화 종료 - 신규 삽입: {success_count}건, 정보 업데이트: {update_count}건, 중복 스킵: {skip_count}건"
        }
        logs_ref.add(log_payload)

        print("\n" + "=" * 60)
        print(f"🏁 파이프라인 연동 완수! [신규 삽입]: {success_count}건 | [정보 업데이트]: {update_count}건 | [중복 스킵]: {skip_count}건")
        print("=" * 60 + "\n")

    except Exception as e:
        error_msg = str(e)
        print(f"\n❌ [에러 발생 및 중단] : {error_msg}")
        logs_ref.add({
            "timestamp": firestore.SERVER_TIMESTAMP,
            "status": "FAILED",
            "task_name": "[crawl_option_expiry_calendar] 국내 옵션만기일 일정 생성",
            "added_count": success_count,
            "updated_count": update_count,
            "skipped_count": skip_count,
            "message": f"옵션만기일 일정 생성기 실패 에러 로그: {error_msg}"
        })


if __name__ == "__main__":
    run_option_expiry_crawler()
