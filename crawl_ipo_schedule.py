import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
import urllib3
import ssl
import re
import sys
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from crawler_schedule import update_my_schedule
from openai import OpenAI

# 정수 변환 제한 확장
sys.set_int_max_str_digits(10000)

# 1. 환경 설정 및 클라이언트 초기화
FIREBASE_KEY_PATH = "stockcalender-13042-firebase-adminsdk-fbsvc-18b1748d9a.json"

if os.path.exists(FIREBASE_KEY_PATH):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = FIREBASE_KEY_PATH
    db = firestore.Client(project="stockcalender-13042")
else:
    print(f"⚠️ [경고] 파이어베이스 인증 파일({FIREBASE_KEY_PATH})을 찾을 수 없습니다.")
    print("로컬 드라이런 모드로 계속 진행합니다.")
    db = None

openai_key = os.environ.get("OPENAI_API_KEY")
if openai_key:
    ai_client = OpenAI(api_key=openai_key)
else:
    ai_client = None
    print("⚠️ [경고] OPENAI_API_KEY 환경 변수가 없습니다. GPT 요약 기능은 생략됩니다.")

events_ref = db.collection("events") if db else None
logs_ref = db.collection("crawler_logs") if db else None


def has_confirmed_price(detail_text: str) -> bool:
    """detail 텍스트에 '미정'이 아닌 실제 확정 공모가가 이미 기록되어 있는지 여부"""
    return "공모가:" in detail_text and "공모가: 미정" not in detail_text


def clean_text(text):
    """인코딩 깨짐 및 유령 공백 문자를 완전히 박멸하는 함수"""
    if not text:
        return ""
    cleaned = re.sub(r'[\s\xa0\xad\t\n\r]+', ' ', text)
    return cleaned.strip()


def summarize_business_with_ai(stock_name, business_raw_text):
    if not ai_client:
        return "신규상장 예정 종목"
    if not business_raw_text or "사업현황" not in business_raw_text:
        return "참조 가능한 정보 없음"

    try:
        response = ai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "너는 주식 증권사 리서치 애널리스트야. 제공된 공모 기업의 사업 현황을 읽고, "
                        "증권사 리포트에서 사용하는 사업 분야를 한 줄로 요약해줘.\n"
                        "조건:\n1. 핵심 기술과 적용 산업만 포함한다.\n2. 고객사는 제외한다.\n"
                        "3. 주어는 사용하지 않는다.\n4. 기업명은 포함하지 않는다.\n"
                        "5. 명사형으로 끝낸다.\n6. 10~20자 내외로 작성한다.\n"
                        "7. '인공지능'키워드는 'AI'로 대체해줘\n8. 설명 없이 결과 한 줄만 출력한다."
                    )
                },
                {"role": "user", "content": f"기업명: {stock_name}\n\n[사업 현황 원문]\n{business_raw_text[:2500]}"}
            ],
            max_tokens=100,
            temperature=0.4
        )
        ai_result = response.choices[0].message.content.strip()
        ai_result = re.sub(r'^[\s\-\•\.\,\_]+', '', ai_result)
        return f"{ai_result}"
    except Exception as e:
        print(f"❌ GPT API 통신 실패: {str(e)}")
        return "신규상장 예정 종목"


class SSL38Adapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.set_ciphers('DEFAULT:@SECLEVEL=0')
        kwargs['ssl_context'] = context
        return super(SSL38Adapter, self).init_poolmanager(*args, **kwargs)


def run_stock_crawler():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 GPT AI 엔진 장착 크롤러 가동 (38커뮤니케이션 전용 SSL 튜닝 패치 적용)...")

    base_url = "https://www.38.co.kr"
    list_url = f"{base_url}/html/fund/index.htm?o=nw"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
    }

    # 💡 [디버깅 모니터링 엔진] 수집 결과 기록용 동적 버킷 정의
    processed_list = []
    skipped_list = []

    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        session = requests.Session()
        session.mount("https://", SSL38Adapter())

        response = session.get(list_url, headers=headers, verify=False, timeout=15)
        response.encoding = 'euc-kr'

        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"summary": "신규상장종목"})
        if not table:
            raise Exception("상장 일정 테이블 요소를 찾을 수 없습니다.")

        rows = table.find_all("tr")
        success_count = 0
        update_count = 0
        skip_count = 0
        current_year = datetime.now().year

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 4:
                continue

            stock_a = cols[0].find("a")
            if not stock_a or not stock_a.get("href"):
                continue

            stock_name = clean_text(stock_a.text)
            raw_date = clean_text(cols[1].text).replace(".", "/")

            if not stock_name or not raw_date or raw_date in ["-", "미정", "상장일"] or "종목명" in stock_name:
                continue

            stock_name = stock_name.split("(")[0].strip()
            stock_name = stock_name.replace("(주)", "").replace("주식회사", "").strip()

            try:
                if len(raw_date.split('/')[0]) == 4:
                    date_obj = datetime.strptime(raw_date, "%Y/%m/%d")
                else:
                    date_obj = datetime.strptime(f"{current_year}/{raw_date}", "%Y/%m/%d")
                formatted_date = date_obj.strftime("%Y-%m-%d")
            except ValueError:
                continue

            # 중복 검증 단계
            # 💡 이미 등록된 일정이라도 '확정 공모가'가 아직 없는 경우엔 매일 재크롤링하며
            #    공모가가 새로 발표되었는지 계속 확인해야 하므로, 무조건 스킵하지 않고
            #    기존 문서(existing_doc)를 들고 다음 단계(상세 페이지 조회)로 넘어간다.
            existing_doc = None
            existing_detail = ""
            if events_ref:
                duplicate_query = events_ref.where(
                    filter=FieldFilter("date", "==", formatted_date)
                ).where(
                    filter=FieldFilter("eventName", "==", stock_name)
                ).get()

                if len(duplicate_query) > 0:
                    existing_doc = duplicate_query[0]
                    existing_detail = existing_doc.to_dict().get("detail") or ""

                    if has_confirmed_price(existing_detail):
                        skip_count += 1
                        # 💡 스킵된 종목 기록 저장
                        skipped_list.append({
                            "date": formatted_date,
                            "name": stock_name,
                            "reason": "DB 내 중복 + 확정 공모가 이미 존재 (스킵 처리)"
                        })
                        continue

                    print(f"⏳ [공모가 미확정] {stock_name} - 기존 일정에 확정 공모가가 없어 최신 정보 재확인 진행")

            detail_route = stock_a.get("href")
            if "index.htm" in detail_route:
                detail_route = detail_route.replace("index.htm", "").replace("?", "")

            detail_url = base_url + detail_route if detail_route.startswith(
                "/") else f"{base_url}/html/fund/{detail_route}"
            if "o=v" not in detail_url and "no=" in detail_url:
                detail_url = detail_url.replace("?", "?o=v&")

            print(f"\n🔍 [디버깅 대상 종목 시작] -------------------------------")
            print(f"🏢 종목명: {stock_name} | 🔗 URL: {detail_url}")

            if existing_doc:
                # 💡 이미 등록된 종목의 공모가 재확인 케이스: 기존에 AI가 요약해둔 사업 요약 첫 줄은
                #    그대로 재사용하고(비용 절감), 공모가/유통 정보만 최신화한다.
                detail_desc = existing_detail.split("\n")[0].strip() if existing_detail else "신규상장 예정 종목"
            else:
                detail_desc = "신규상장 예정 종목"
            confirmed_price = ""
            floating_shares = ""
            floating_amount = ""

            try:
                detail_res = session.get(detail_url, headers=headers, verify=False, timeout=15)
                detail_res.encoding = 'euc-kr'

                if detail_res.status_code == 200:
                    detail_soup = BeautifulSoup(detail_res.text, "html.parser")

                    # 1. 확정공모가 추적
                    detail_tables = detail_soup.find_all("table")
                    for d_table in detail_tables:
                        if "확정공모가" in d_table.get_text():
                            d_rows = d_table.find_all("tr")
                            for d_row in d_rows:
                                d_cells = d_row.find_all(["th", "td"])
                                for i, cell in enumerate(d_cells):
                                    if "확정공모가" in cell.get_text():
                                        if i + 1 < len(d_cells):
                                            raw_price = d_cells[i + 1].get_text().strip()
                                            if len(raw_price) < 30:
                                                price_digits = re.sub(r'[^\d]', '', raw_price)
                                                if price_digits:
                                                    confirmed_price = f"{int(price_digits):,}원"
                                        break

                    # 2. 유통가능물량 진짜 표 저격 엔진
                    for idx, d_table in enumerate(detail_tables):
                        table_rows = d_table.find_all("tr")
                        if len(table_rows) < 3:
                            continue

                        header_chunk_text = "".join([re.sub(r'\s+', '', r.get_text()) for r in table_rows[:4]])

                        if "유통가능물량" in header_chunk_text and "주식수" in header_chunk_text and "지분율" in header_chunk_text:
                            print(f"  👉 [{idx}번 테이블] 3종 키워드 스캔으로 진짜 유통 표 저격 적중")
                            table_summary = re.sub(r'\s+', ' ', d_table.get_text()).strip()
                            print(f"    - [표 본문 스냅샷]: '{table_summary[:120]}...'")

                            for d_row in reversed(table_rows):
                                d_cells = d_row.find_all(["th", "td"])
                                if not d_cells:
                                    continue

                                cells_list = [re.sub(r'\s+', '', cell.get_text()) for cell in d_cells]
                                row_split_text = "|".join(cells_list)

                                is_summary_row = any(
                                    item.endswith("합계") or item.endswith("총계") for item in cells_list if item)
                                has_percentage = any("%" in item for item in cells_list)

                                is_financial_noise = any(
                                    k in row_split_text for k in ["과목", "자본", "매출", "이익", "손실", "자산총계", "부채총계", "거래등"])

                                if is_summary_row and has_percentage and not is_financial_noise:
                                    print(f"    - '진짜 유통 합계' 결산 행 안전 분할 포착: '{row_split_text}'")

                                    valid_items = []
                                    for item in cells_list:
                                        if re.match(r'^[\d,]+$', item) or re.match(r'^[\d.]+\%$', item):
                                            valid_items.append(item)

                                    print(f"    - 구분 정제된 유효 스펙 데이터 배열: {valid_items}")

                                    if len(valid_items) >= 2:
                                        final_percent = valid_items[-1]
                                        final_shares = valid_items[-2]

                                        if final_percent != "100.00%":
                                            floating_shares = f"{final_shares}주({final_percent})"
                                            print(f"    - 🎯 완벽 수집 성공 결과: {floating_shares}")
                                            break

                            if floating_shares:
                                break

                    # 3. OpenAI 요약 파트 (신규 종목일 때만 수행 - 기존 종목은 저장된 요약을 재사용)
                    page_text = detail_soup.get_text()
                    if not existing_doc and ("1." in page_text or "사업현황" in page_text):
                        cleaned_page_text = clean_text(page_text)
                        start_idx = cleaned_page_text.find("1.")
                        if start_idx == -1:
                            start_idx = cleaned_page_text.find("사업현황")

                        if start_idx != -1:
                            target_chunk = cleaned_page_text[max(0, start_idx - 20):start_idx + 3500]
                            detail_desc = summarize_business_with_ai(stock_name, target_chunk)
                            print(f"🤖 OpenAI 한줄 요약 변환: {detail_desc}")
            except Exception as sub_e:
                print(f"❌ {stock_name} 상세 데이터 가공 중 에러 발생: {str(sub_e)}")

            # 금액 연산 파트
            if confirmed_price and floating_shares:
                try:
                    num_price = int(re.sub(r'[^\d]', '', confirmed_price))
                    num_shares = int(re.sub(r'[^\d]', '', floating_shares.split('주')[0]))
                    calc_amount_billion = round((num_price * num_shares) / 100000000)
                    if calc_amount_billion > 0:
                        color_circle = "🔴" if calc_amount_billion >= 1000 else "🔵"
                        floating_amount = f"약 {calc_amount_billion:,}억 원 {color_circle}"
                except Exception as calc_err:
                    print(f"⚠️ {stock_name} 유통가능액수 수식 연산 오류: {str(calc_err)}")

            # 구조 결합
            # 💡 확정 공모가가 아직 없는 경우에도 '미정' 상태를 명시적으로 남겨서
            #    다음 크롤링에서 공모가가 새로 확정됐는지 계속 재확인할 수 있게 한다.
            if confirmed_price:
                detail_desc = f"{detail_desc}\n공모가: {confirmed_price}"
            else:
                detail_desc = f"{detail_desc}\n공모가: 미정"

            if floating_shares:
                detail_desc = f"{detail_desc}\n유통가능물량: {floating_shares}"

            if floating_amount:
                detail_desc = f"{detail_desc}\n유통가능액수: {floating_amount}"
            elif not confirmed_price:
                detail_desc = f"{detail_desc}\n유통가능액수: 공모가 확정 대기중"

            if existing_doc:
                # 💡 기존에 등록된 일정 - detail 내용에 실질적인 변화(공모가 확정 등)가 있을 때만 갱신한다.
                if detail_desc == existing_detail:
                    skip_count += 1
                    skipped_list.append({
                        "date": formatted_date,
                        "name": stock_name,
                        "reason": "확정 공모가 아직 미발표 (다음 회차에 재확인)"
                    })
                    print(f"⏭️ 확정 공모가 아직 미발표 - 다음 크롤링에서 재확인 예정")
                    print(f"🔍 [디버깅 대상 종목 종료] -------------------------------\n")
                    continue

                if events_ref:
                    existing_doc.reference.update({"detail": detail_desc})
                    print(f"🔄 확정 공모가 신규 발표 감지 - 기존 일정 detail 갱신 완료")
                else:
                    print(f"📝 [드라이런 모드 로그 출력] 공모가 갱신 대상 날짜: {formatted_date} | 종목: {stock_name}")
                update_count += 1
            else:
                if events_ref:
                    payload = {
                        "date": formatted_date,
                        "category": "신규상장",
                        "eventName": stock_name,
                        "detail": detail_desc,
                        "relatedStocks": "",
                        "url": detail_url
                    }
                    events_ref.add(payload)
                    print(f"✅ 데이터 최종 적재 성공 완료")
                else:
                    print(f"📝 [드라이런 모드 로그 출력] 날짜: {formatted_date} | 종목: {stock_name}")
                success_count += 1

            # 💡 정상 처리 완료 리스트 기록 추가
            processed_list.append({
                "date": formatted_date,
                "name": stock_name,
                "detail": detail_desc.replace("\n", " | ")
            })
            print(f"🔍 [디버깅 대상 종목 종료] -------------------------------\n")

        # 💡 [핵심 교정부] 최종 요약 현황 테이블 콘솔 전수 출력
        print("\n" + "=" * 80)
        print("📊 [최종 실행 보고서] 상장 일정 파이프라인 정밀 분석 결과")
        print("=" * 80)

        print(f"\n✅ 1. 파이어베이스 신규 적재 목록 (총 {len(processed_list)}건)")
        print("-" * 80)
        if not processed_list:
            print("  [알림] 새로 추가된 일정이 존재하지 않습니다.")
        else:
            for idx, item in enumerate(processed_list, start=1):
                print(f"  [{idx:02d}] 상장일: {item['date']} | 종목명: {item['name']}")
                print(f"       ↳ 데이터 요약: {item['detail']}")
        print("-" * 80)

        print(f"\n⏭️  2. 데이터베이스 중복 스킵 목록 (총 {len(skipped_list)}건)")
        print("-" * 80)
        if not skipped_list:
            print("  [알림] 중복으로 인한 스킵 건수가 없습니다. 깨끗한 원천 데이터 상태입니다.")
        else:
            for idx, item in enumerate(skipped_list, start=1):
                print(f"  [{idx:02d}] 등록일: {item['date']} | 종목명: {item['name']:<12} ➡️ 사유: {item['reason']}")
        print("-" * 80)

        if logs_ref:
            log_payload = {
                "timestamp": firestore.SERVER_TIMESTAMP,
                "status": "SUCCESS",
                "task_name": "[crawl_ipo_schedule] 38커뮤니케이션 IPO일정 수집",
                "added_count": success_count,
                "updated_count": update_count,
                "skipped_count": skip_count,
                "message": f"AI 수집 자동화 정상 종료 - 신규: {success_count}건, 공모가 갱신: {update_count}건, 중복 스킵: {skip_count}건"
            }
            logs_ref.add(log_payload)
        print(f"\n🏁 AI 파이프라인 프로세스 종료. 추가: {success_count}건 / 공모가 갱신: {update_count}건 / 스킵: {skip_count}건")
        print("=" * 80 + "\n")

    except Exception as e:
        error_msg = str(e)
        print(f"❌ 에러 발생: {error_msg}")
        if logs_ref:
            logs_ref.add({
                "timestamp": firestore.SERVER_TIMESTAMP,
                "status": "FAILED",
                "task_name": "[crawl_ipo_schedule] 38커뮤니케이션 IPO일정 수집",
                "added_count": 0,
                "skipped_count": 0,
                "message": f"크롤링 실패 에러 로그: {error_msg}"
            })


if __name__ == "__main__":
    # 실행 시 이 배치 자신의 다음 실행 예정시간만 Firestore(crawler_schedules)에 기록
    update_my_schedule(db, __file__, display_name="38커뮤니케이션 IPO일정 수집")

    run_stock_crawler()