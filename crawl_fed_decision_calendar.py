import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os
import urllib3
import ssl
import re
import sys
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from crawler_schedule import update_my_schedule

# 정수 변환 제한 확장
sys.set_int_max_str_digits(10000)

# 파이어베이스 엔진 초기화
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "stockcalender-13042-firebase-adminsdk-fbsvc-18b1748d9a.json"
db = firestore.Client()
events_ref = db.collection("events")
logs_ref = db.collection("crawler_logs")


def run_fed_crawler():
    # 실행 시점의 당해 연도(올해)와 차기 연도(내년)를 자동으로 계산합니다.
    current_year_int = datetime.now().year
    current_year = str(current_year_int)
    next_year = str(current_year_int + 1)

    print("\n" + "=" * 60)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 연준(FED) 공식 FOMC 일정 크롤러 가동")
    print(f"🎯 동적 타겟팅 가동: 올해({current_year}년) 및 내년({next_year}년) 실시간 DB 동기화")
    print("=" * 60)

    url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    class SSLAdapter(requests.adapters.HTTPAdapter):
        def init_poolmanager(self, *args, **kwargs):
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            context.set_ciphers('DEFAULT:@SECLEVEL=0')
            kwargs['ssl_context'] = context
            return super(SSLAdapter, self).init_poolmanager(*args, **kwargs)

    success_count = 0
    update_count = 0
    skip_count = 0

    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        session = requests.Session()
        session.mount("https://", SSLAdapter())

        print("🌐 1. 미 연방준비제도이사회(FRB) FOMC 공식 웹 페이지 로드 중...")
        response = session.get(url, headers=headers, verify=False, timeout=15)

        if response.status_code != 200:
            raise Exception(f"FOMC 페이지 접근 실패 (상태 코드: {response.status_code})")

        html_content = response.text
        soup = BeautifulSoup(html_content, "html.parser")

        all_parsed_events = []

        # 영어 월 이름을 두 자리 숫자 포맷("01" ~ "12")으로 치환하기 위한 매퍼 정의
        months_numeric_map = {
            "January": "01", "February": "02", "March": "03", "April": "04", "May": "05", "June": "06",
            "July": "07", "August": "08", "September": "09", "October": "10", "November": "11", "December": "12",
            "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "Jun": "06", "Jul": "07", "Aug": "08", "Sept": "09",
            "Oct": "10", "Nov": "11", "Dec": "12",
            "Jan/Feb": "01", "Apr/May": "04", "Oct/Nov": "10"
        }

        print("\n🔍 2. 연도별 패널 순회 및 일정 정밀 추출/포맷팅")
        print("-" * 60)

        panels = soup.find_all("div", class_=lambda x: x and "panel" in x and "panel-default" in x)
        print(f"📦 발견된 전체 연도별 패널 수: {len(panels)}개")

        for panel in panels:
            heading = panel.find(class_=lambda x: x and "panel-heading" in x)
            if not heading:
                continue

            heading_text = heading.get_text()

            target_year = None
            if current_year in heading_text:
                target_year = current_year
            elif next_year in heading_text:
                target_year = next_year
            else:
                continue

            print(f"📅 [{target_year}년 FOMC 패널 매칭 성공 - 파싱 시작]")

            meeting_rows = panel.find_all("div", class_=lambda x: x and "fomc-meeting" in x)

            valid_count = 0
            for row in meeting_rows:
                if row.find_parent("div", class_=lambda x: x and "fomc-meeting" in x):
                    continue

                month_div = row.find("div", class_=lambda x: x and "month" in x)
                day_div = row.find("div", class_=lambda x: x and "date" in x)

                if not month_div or not day_div:
                    cols = row.find_all("div", class_=re.compile(r"col-"))
                    if len(cols) >= 2:
                        month_text = cols[0].get_text(strip=True)
                        day_text = cols[1].get_text(strip=True)
                    else:
                        continue
                else:
                    month_text = month_div.get_text(strip=True)
                    day_text = day_div.get_text(strip=True)

                # 월 가공 및 숫자 변환
                month_cleaned = month_text.replace(".", "").strip()
                month_cleaned = re.sub(r'\s+', ' ', month_cleaned)

                month_num = months_numeric_map.get(month_cleaned)
                if not month_num:
                    continue

                formatted_month = f"{month_num}월"

                # 일 가공 및 특수 문자 정제
                day_cleaned = re.sub(r'[^0-9\-/\s|]', '', day_text).strip()
                if not day_cleaned or not re.search(r'\d+', day_cleaned):
                    continue

                # '27-28' 과 같이 범위형으로 정의된 날짜인 경우 뒤의 일자('28')만 안전하게 추출
                if '-' in day_cleaned:
                    days_split = day_cleaned.split('-')
                    target_day = days_split[-1].strip()
                else:
                    target_day = day_cleaned

                if not target_day.isdigit():
                    continue

                formatted_day = f"{int(target_day):02d}일"

                event_key = f"{target_year}-{formatted_month}-{formatted_day}"
                if not any(f"{ev['year']}-{ev['month']}-{ev['days']}" == event_key for ev in all_parsed_events):
                    all_parsed_events.append({
                        "year": target_year,
                        "month": formatted_month,
                        "days": formatted_day,
                        "raw_info": f"{target_year}년 {formatted_month} {formatted_day}"
                    })
                    valid_count += 1

            print(f"  ↳ {target_year}년 패널에서 순수 일정 {valid_count}개 정제 완료.")

        # 정렬 (연도 순 -> 월 순 -> 일 순)
        def sort_key(x):
            m_val = int(x["month"].replace("월", ""))
            d_val = int(x["days"].replace("일", ""))
            return (x["year"], m_val, d_val)

        all_parsed_events.sort(key=sort_key)

        # 수집 완료 후 전수 출력
        print(f"\n📊 3. 수집 완료된 FOMC 일정 콘솔 전수 출력 ({current_year}년 ~ {next_year}년)")
        print("-" * 60)
        if not all_parsed_events:
            print("❌ 조건에 맞는 FOMC 일정을 수집하지 못했습니다.")
        else:
            for idx, ev in enumerate(all_parsed_events, start=1):
                print(f"  [{idx:02d}] {ev['raw_info']} (대상 연도: {ev['year']})")
        print("-" * 60)

        print("\n🔥 4. 파이어베이스 Firestore 데이터베이스 업로드 작업 시작...")
        print("=" * 60)

        category_name = "일반"
        final_event_name = "미국 FED 연준 금리결정"

        for ev in all_parsed_events:
            clean_m = ev["month"].replace("월", "")
            clean_d = ev["days"].replace("일", "")
            db_date_str = f"{ev['year']}-{clean_m}-{clean_d}"

            # FOMC 금리결정은 미국 동부시간 오후 2시 발표 = 한국시간 다음날 새벽 3시
            final_detail = "한국시간 기준 다음날 새벽 3시"

            existing_docs = events_ref.where(
                filter=FieldFilter("date", "==", db_date_str)
            ).where(
                filter=FieldFilter("category", "==", category_name)
            ).where(
                filter=FieldFilter("eventName", "==", final_event_name)
            ).get()

            if len(existing_docs) > 0:
                doc = existing_docs[0]
                existing_data = doc.to_dict()

                # detail이 최신이고 이미 검증 표시(isVerified)까지 되어 있으면 쓰기 생략(중복 스킵)
                if final_detail == existing_data.get("detail") and existing_data.get("isVerified") is True:
                    print(f"⏭️  [중복 스킵] 날짜: {db_date_str} | 이미 존재합니다.")
                    skip_count += 1
                    continue

                # detail/검증 표시 갱신
                doc.reference.update({
                    "detail": final_detail,
                    "isVerified": True
                })
                update_count += 1
                print(f"🔄  [정보 업데이트] 날짜: {db_date_str} | detail/검증표시를 갱신했습니다.")
            else:
                # 완전 신규인 경우 컬렉션 추가
                # FOMC는 공식 일정이라 별도 크로스체크 불필요 → isVerified=True(사실 확인 완료)로 저장
                payload = {
                    "date": db_date_str,
                    "category": category_name,
                    "eventName": final_event_name,
                    "detail": final_detail,
                    "relatedStocks": "",
                    "url": "",
                    "isVerified": True
                }
                events_ref.add(payload)
                success_count += 1
                print(f"✅  [신규 삽입] 날짜: {db_date_str} | 금리결정 일정을 신규 등록했습니다.")

        log_payload = {
            "timestamp": firestore.SERVER_TIMESTAMP,
            "status": "SUCCESS",
            "task_name": "[crawl_fed_decision_calendar] 美 연준 금리결정 수집",
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
            "task_name": "[crawl_fed_decision_calendar] 美 연준 금리결정 수집",
            "added_count": success_count,
            "updated_count": update_count,
            "skipped_count": skip_count,
            "message": f"FED 크롤러 실패 에러 로그: {error_msg}"
        })


if __name__ == "__main__":
    # 실행 시 이 배치 자신의 다음 실행 예정시간만 Firestore(crawler_schedules)에 기록
    update_my_schedule(db, __file__, display_name="美 연준 금리결정 수집")

    # cron('0 0 1 * *')이 UTC·KST 모두 1일이라 별도 날짜 가드 없이 바로 실행
    run_fed_crawler()