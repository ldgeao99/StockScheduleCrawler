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

# 1. 환경 설정 및 클라이언트 초기화
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "stockcalender-13042-firebase-adminsdk-fbsvc-18b1748d9a.json"
db = firestore.Client()
events_ref = db.collection("events")
logs_ref = db.collection("crawler_logs")


def clean_text(text):
    if not text:
        return ""
    cleaned = re.sub(r'[\s\xa0\xad\t\n\r]+', ' ', text)
    return cleaned.strip()


def parse_date(date_text):
    """'2026년 07월 14일' 형식을 '2026-07-14' 형태로 변환"""
    date_numbers = re.findall(r'\d+', date_text)
    if len(date_numbers) >= 3:
        try:
            year = int(date_numbers[0])
            month = int(date_numbers[1])
            day = int(date_numbers[2])
            return f"{year:04d}-{month:02d}-{day:02d}"
        except ValueError:
            return None
    return None


def run_holiday_crawler():
    print("\n" + "=" * 60)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 글로벌 증시 휴장 크롤러 가동 (디버깅 모드)")
    print("=" * 60)

    # 요청 타겟 URL
    url = "https://kr.investing.com/holiday-calendar/"

    target_countries = {
        "한국": "한국",
        "미국": "미국",
        "중국": "중국",
        "대만": "대만",
        "일본": "일본"
    }

    # 💡 [요구사항 반영] 국가별 정렬 우선순위 스코어 매핑 선언
    country_sort_priority = {
        "한국": 0,
        "미국": 1,
        "중국": 2,
        "대만": 3,
        "일본": 4
    }

    # 예외 발생 시 except 블록에서 안전하게 참조할 수 있도록 카운트 변수를 최상단에 미리 선언
    success_count = 0
    skip_count = 0

    try:
        print("🌐 1. 인베스팅닷컴에 보안 우회 및 브라우저 위장 요청 전송하는 중...")

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.google.com/",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-User": "?1"
        }

        class SSLAdapter(requests.adapters.HTTPAdapter):
            def init_poolmanager(self, *args, **kwargs):
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                context.set_ciphers('DEFAULT:@SECLEVEL=0')
                kwargs['ssl_context'] = context
                return super(SSLAdapter, self).init_poolmanager(*args, **kwargs)

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        session = requests.Session()
        session.mount("https://", SSLAdapter())

        response = session.get(url, headers=headers, verify=False, timeout=15)

        try:
            html_content = response.content.decode('utf-8')
        except UnicodeDecodeError:
            response.encoding = 'utf-8'
            html_content = response.text

        if response.status_code != 200:
            raise Exception(f"HTTP 요청 실패 (상태 코드: {response.status_code})")
        print(f"📡 HTTP 응답 성공 (코드: {response.status_code}) | 데이터 길이: {len(html_content)} bytes")

        print(f"🔎 HTML 서두 테스트 (정상 여부 검증용): {html_content[:150].strip()}...")

        soup = BeautifulSoup(html_content, "html.parser")

        print("\n🔍 2. HTML 내 표 구조(tbody) 탐색 시도...")
        tbody = soup.find("tbody")
        if not tbody:
            table = soup.find("table", {"id": "holidayCalendarTable"}) or soup.find("table", class_=re.compile(r"genTbl"))
            if table:
                tbody = table.find("tbody")

        if not tbody:
            print("⚠️ [위험] tbody 요소를 전혀 찾지 못했습니다. 상위 1000자 HTML 스냅샷을 출력합니다:")
            print(f"{html_content[:1000]}")
            raise Exception("휴장 일정의 tbody 요소를 찾을 수 없습니다.")

        rows = tbody.find_all("tr")
        print(f"📊 표 내부에서 총 {len(rows)}개의 행(tr)을 감지했습니다. 파싱을 시작합니다.\n")
        print("-" * 60)

        daily_groups = {}
        last_valid_date_str = ""

        for idx, row in enumerate(rows, start=1):
            cols = row.find_all("td")
            if len(cols) < 4:
                continue

            # A. 날짜 탐색 및 백업
            date_td = cols[0]
            raw_date_text = clean_text(date_td.get_text())

            if raw_date_text:
                parsed_date = parse_date(raw_date_text)
                if parsed_date:
                    last_valid_date_str = parsed_date
                    print(f"\n📅 [날짜 감지] {raw_date_text} ➡️ {last_valid_date_str} 로 날짜 포인터 설정")

            if not last_valid_date_str:
                continue

            # B. 국가 데이터 가공
            country_td = cols[1]
            country_name = ""

            a_tag = country_td.find("a")
            img_tag = country_td.find("img")
            span_tag = country_td.find("span")

            if a_tag:
                country_name = clean_text(a_tag.text)
            elif img_tag and img_tag.get("title"):
                country_name = clean_text(img_tag.get("title"))
            elif img_tag and img_tag.get("alt"):
                country_name = clean_text(img_tag.get("alt"))
            elif span_tag and span_tag.text.strip():
                country_name = clean_text(span_tag.text)
            else:
                country_name = clean_text(country_td.text)

            # C. 타겟 국가 필터링 검사
            matched_country_short = None
            for target_k, short_v in target_countries.items():
                if target_k in country_name:
                    matched_country_short = short_v
                    break

            market_name = clean_text(cols[2].text)
            holiday_reason = clean_text(cols[3].text)

            status_icon = "🎯 [매칭성공]" if matched_country_short else "⏭️ [스킵대상]"
            print(f"  {status_icon} 행 #{idx:<3} | 국가: {country_name:<7} | 거래소: {market_name:<15} | 사유: {holiday_reason}")

            if not matched_country_short or not holiday_reason:
                continue

            # D. 날짜별 메모리 그룹화 처리
            if last_valid_date_str not in daily_groups:
                daily_groups[last_valid_date_str] = []

            is_already_added = any(
                item["country"] == matched_country_short and item["reason"] == holiday_reason
                for item in daily_groups[last_valid_date_str]
            )

            if not is_already_added:
                daily_groups[last_valid_date_str].append({
                    "country": matched_country_short,
                    "reason": holiday_reason,
                    "market": market_name
                })
                print(f"     ➡️ 📥 {last_valid_date_str} 캘린더 그룹에 '{matched_country_short}' 정보 임시 적재 완료.")
            else:
                print(f"     ➡️ ⚠️ 중복 데이터 감지되어 스킵합니다.")

        print("-" * 60)

        # 💡 [요구사항 반영] 메모리에 수집된 그룹 내부 국가 정렬 로직 집행
        print("\n🔮 2-1. [우선순위 정렬 연산] 한국 > 미국 > 중국 > 대만 > 일본 정렬 규칙 가동...")
        for target_date, holiday_list in daily_groups.items():
            if holiday_list:
                holiday_list.sort(key=lambda x: country_sort_priority.get(x["country"], 99))

        print(f"\n📦 3. 메모리 내 최종 그룹화 결과 (총 {len(daily_groups)}일의 일정 수집됨)")
        for d, items in daily_groups.items():
            formatted_items = ", ".join([f"{i['country']}({i['reason']})" for i in items])
            print(f"  • {d} : [{formatted_items}]")

        print("\n📢 3-1. [데이터 검증] 파이어베이스 입력 예정 매칭 데이터 세부 목록")
        print("=" * 60)
        matched_count_total = 0
        for target_date, holiday_list in daily_groups.items():
            if not holiday_list:
                continue

            holiday_details_list = []
            for item in holiday_list:
                holiday_details_list.append(f"{item['country']}({item['reason']})")

            final_event_name = ", ".join(holiday_details_list) + " 증시 휴장"

            print(f"📌 날짜: {target_date} | 이벤트명: {final_event_name}")
            for item in holiday_list:
                print(f"   └ 국가: {item['country']} | 사유: {item['reason']} | 거래소: {item['market']}")
                matched_count_total += 1
        print(f"👉 총 {matched_count_total}개의 개별 일정(국가별)이 {len(daily_groups)}개의 날짜 문서로 압축되어 전송될 예정입니다.")
        print("=" * 60)

        print("\n🔥 4. 파이어베이스 Firestore 데이터베이스 업로드/업데이트 작업 시작...")

        for target_date, holiday_list in daily_groups.items():
            if not holiday_list:
                continue

            holiday_details_list = []
            for item in holiday_list:
                holiday_details_list.append(f"{item['country']}({item['reason']})")

            final_event_name = ", ".join(holiday_details_list) + " 증시 휴장"
            final_detail = ""
            final_url = ""

            # 파이어베이스 중복 조회
            existing_docs = events_ref.where(
                filter=FieldFilter("date", "==", target_date)
            ).where(
                filter=FieldFilter("category", "==", "휴장")
            ).get()

            print(f"\n  [DB 작업] 날짜: {target_date} | 대상: {final_event_name}")

            if len(existing_docs) > 0:
                doc = existing_docs[0]
                existing_data = doc.to_dict()

                # 완전히 동일한 정보인 경우 업데이트 트랜잭션 건너뛰기
                if final_event_name == existing_data.get("eventName") and final_detail == existing_data.get("detail") and final_url == existing_data.get("url"):
                    print(f"     ⏭️  동일한 데이터가 이미 존재합니다. (클라우드 업데이트 건너뜀)")
                    skip_count += 1
                    continue

                doc.reference.update({
                    "eventName": final_event_name,
                    "detail": final_detail,
                    "url": final_url
                })
                success_count += 1
                print(f"     🔄  내용 변경 감지! 파이어베이스 기존 문서를 성공적으로 업데이트했습니다.")
            else:
                payload = {
                    "date": target_date,
                    "category": "휴장",
                    "eventName": final_event_name,
                    "detail": final_detail,
                    "relatedStocks": "",
                    "url": final_url
                }
                events_ref.add(payload)
                success_count += 1
                print(f"     ✅  새로운 일정을 파이어베이스에 최종 적재했습니다.")

        log_payload = {
            "timestamp": firestore.SERVER_TIMESTAMP,
            "status": "SUCCESS",
            "task_name": "[crawl_investing_holiday_calendar] 글로벌 증시 휴장 수집",
            "added_count": success_count,
            "skipped_count": skip_count,
            "message": f"디버깅 실행 종료 - 처리: {success_count}건, 보존스킵: {skip_count}건"
        }
        logs_ref.add(log_payload)

        print("\n" + "=" * 60)
        print(f"🏁 디버깅 가동 프로세스 완료! 최종 추가/갱신: {success_count}건 | 스킵: {skip_count}건")
        print("=" * 60 + "\n")

    except Exception as e:
        error_msg = str(e)
        print(f"\n❌ [에러 발생 및 중단] : {error_msg}")
        logs_ref.add({
            "timestamp": firestore.SERVER_TIMESTAMP,
            "status": "FAILED",
            "task_name": "[crawl_investing_holiday_calendar] 글로벌 증시 휴장 수집",
            "added_count": success_count,
            "skipped_count": skip_count,
            "message": f"디버깅 모드 실패 에러 로그: {error_msg}"
        })


if __name__ == "__main__":
    # 실행 시 이 배치 자신의 다음 실행 예정시간만 Firestore(crawler_schedules)에 기록
    update_my_schedule(db, __file__)

    # cron('0 0 1 * *')이 UTC·KST 모두 1일이라 별도 날짜 가드 없이 바로 실행
    run_holiday_crawler()