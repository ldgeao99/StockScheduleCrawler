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

# FedWatch 확률은 매일 변하는 스냅샷 데이터라 일정용 events 와 분리된 전용 컬렉션에 적재
fedwatch_ref = db.collection("fedwatch_probabilities") if db else None
logs_ref = db.collection("crawler_logs") if db else None

TASK_NAME = "[crawl_fedwatch_probabilities] 美 FedWatch 금리확률 수집"

# 최근 보관 기간(일) - 이 일수보다 오래된 스냅샷은 삭제하여 최근 1달치만 유지
RETENTION_DAYS = 30

# QuikStrike(CME FedWatch 실제 데이터 제공 앱) 접근 파라미터
QS_BASE = "https://cmegroup-tools.quikstrike.net/User/"
# Referer 가 cmegroup.com 이 아니면 "QuikStrike Error" 가 반환되므로 반드시 지정해야 함
REFERER = "https://www.cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html"
VIEW_ITEM = "IntegratedFedWatchTool"
USER_ID = "lwolf"  # CME 가 iframe 에 심어둔 공개 공용 세션 ID (로그인 불필요)

MONTHS = {m: f"{i:02d}" for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}


def _to_iso(date_str):
    """'16 Sep 2026' -> '2026-09-16' 형식으로 표준화"""
    m = re.match(r'(\d{1,2})\s+([A-Za-z]{3})[a-z]*\s+(\d{4})', date_str.strip())
    if not m:
        return date_str.strip()
    d, mon, y = m.group(1), m.group(2)[:3].title(), m.group(3)
    return f"{y}-{MONTHS.get(mon, '00')}-{int(d):02d}"


def fetch_fedwatch_nearest():
    """가장 가까운 FOMC 회의의 목표금리별 확률(현재값)을 수집하여 dict 로 반환"""
    session = requests.Session()
    headers = {"Referer": REFERER}

    # 1단계: 세션(insid/qsid) 발급. QuikStrikeTools.aspx 가 302 로 세션 파라미터를 붙여 리다이렉트함
    print("🌐 1. QuikStrike 세션 발급 중 (Referer 우회 접근)...")
    r1 = session.get(QS_BASE + "QuikStrikeTools.aspx",
                     params={"viewitemid": VIEW_ITEM, "userId": USER_ID},
                     headers=headers, impersonate="chrome", timeout=20)
    m = re.search(r'insid=(\d+)&qsid=([0-9a-fA-F\-]+)', r1.url) or \
        re.search(r'insid=(\d+)&qsid=([0-9a-fA-F\-]+)', r1.text)
    if not m:
        raise Exception("QuikStrike 세션(insid/qsid) 발급에 실패했습니다.")
    insid, qsid = m.group(1), m.group(2)
    print(f"🔑 세션 확보 완료 (insid={insid}, qsid={qsid})")

    # 2단계: 실제 데이터가 서버 렌더링된 View 페이지 로드
    print("📡 2. FedWatch 확률 View 페이지 로드 중...")
    r2 = session.get(QS_BASE + "QuikStrikeView.aspx",
                     params={"viewitemid": VIEW_ITEM, "userId": USER_ID,
                             "insid": insid, "qsid": qsid},
                     headers=headers, impersonate="chrome", timeout=20)
    if r2.status_code != 200:
        raise Exception(f"QuikStrikeView 로드 실패 (상태 코드: {r2.status_code})")
    print(f"📥 응답 수신 완료 (길이: {len(r2.text)} bytes)")

    return parse_fedwatch(r2.text)


def parse_fedwatch(html):
    """View 페이지 HTML 에서 목표금리 확률 테이블/회의일/요약을 파싱"""
    soup = BeautifulSoup(html, "html.parser")

    # --- 1) 목표금리 확률 테이블 (Target Rate / Probability) ---
    prob_tbl = None
    for t in soup.find_all("table"):
        txt = t.get_text(" ", strip=True).upper()
        if "TARGET RATE" in txt and "PROBABILITY" in txt:
            prob_tbl = t
            break
    if prob_tbl is None:
        raise Exception("확률 테이블을 찾지 못했습니다. (페이지 구조 변경 가능성)")

    probabilities = []
    current_target = None
    data_as_of = None
    for tr in prob_tbl.find_all("tr"):
        cells = [re.sub(r'\s+', ' ', c.get_text(" ", strip=True))
                 for c in tr.find_all(["th", "td"])]
        cells = [c for c in cells if c != ""]
        if not cells:
            continue
        first = cells[0]
        if first.startswith("*"):  # "* Data as of ... CT"
            data_as_of = first.lstrip("* ").strip()
            continue
        # 밴드 라벨 예: "350-375", "350-375 (Current)"
        m = re.match(r'^(\d+-\d+)\s*(\(Current\))?$', first)
        if not m or len(cells) < 2:
            continue
        band = m.group(1)
        is_current = bool(m.group(2))
        now_val = cells[1].replace("%", "").strip()  # cells[1] = "Now" 컬럼(현재 확률)
        try:
            prob = float(now_val)
        except ValueError:
            continue
        if is_current:
            current_target = band
        # 확률 0% 밴드는 노이즈이므로 현재 밴드를 제외하고 제외
        if prob > 0 or is_current:
            # 현재 밴드는 문자열에 "(Current)"를 병기하여 저장 (예: "350-375 (Current)")
            label = f"{band} (Current)" if is_current else band
            probabilities.append({"targetRate": label,
                                  "probability": prob,
                                  "isCurrent": is_current})

    if not probabilities:
        raise Exception("확률 데이터가 비어 있습니다. (파싱 실패)")

    # --- 2) 회의 정보 테이블에서 가장 가까운 회의일 추출 ---
    meeting_date_iso = None
    for t in soup.find_all("table"):
        txt = t.get_text(" ", strip=True)
        if re.search(r'Meeting Date', txt, re.I) and re.search(r'Contract', txt, re.I):
            mm = re.search(r'(\d{1,2}\s+[A-Za-z]{3,}\s+\d{4})', txt)
            if mm:
                meeting_date_iso = _to_iso(mm.group(1))
            break

    # --- 3) EASE / NO CHANGE / HIKE 요약을 현재 밴드 기준으로 계산 ---
    #  (게이지 UI 는 JS 렌더링이라 정적 HTML 에 없으므로 표에서 직접 산출)
    ease = no_change = hike = 0.0
    if current_target:
        cur_low = int(current_target.split("-")[0])
        for p in probabilities:
            # targetRate 에 "(Current)" 가 병기될 수 있으므로 앞쪽 숫자만 안전하게 추출
            low = int(re.match(r'(\d+)', p["targetRate"]).group(1))
            if low < cur_low:
                ease += p["probability"]
            elif low == cur_low:
                no_change += p["probability"]
            else:
                hike += p["probability"]

    return {
        "meetingDate": meeting_date_iso,
        "currentTargetRate": current_target,
        "dataAsOf": data_as_of,
        "summary": {"ease": round(ease, 1),
                    "noChange": round(no_change, 1),
                    "hike": round(hike, 1)},
        "probabilities": probabilities,
    }


def prune_old_snapshots(crawl_date_str):
    """최근 RETENTION_DAYS 일보다 오래된 스냅샷을 삭제 (최근 1달치만 유지)"""
    if not fedwatch_ref:
        return 0
    cutoff = (datetime.strptime(crawl_date_str, "%Y-%m-%d")
              - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
    old_docs = fedwatch_ref.where(
        filter=FieldFilter("crawlDate", "<", cutoff)
    ).get()
    deleted = 0
    for doc in old_docs:
        doc.reference.delete()
        deleted += 1
    if deleted:
        print(f"🧹 보관기간({RETENTION_DAYS}일) 초과 스냅샷 {deleted}건 삭제 (기준: {cutoff} 이전)")
    return deleted


def run_fedwatch_crawler():
    # 크롤링 시점은 한국시간(KST) 기준 날짜로 기록 (문서 ID = 해당 KST 날짜)
    kst_now = datetime.utcnow() + timedelta(hours=9)
    crawl_date = kst_now.strftime("%Y-%m-%d")

    print("\n" + "=" * 60)
    print(f"[{kst_now.strftime('%Y-%m-%d %H:%M:%S')} KST] 🚀 CME FedWatch 금리확률 크롤러 가동")
    print(f"🎯 대상: 가장 가까운 FOMC 회의 | 저장 컬렉션: fedwatch_probabilities")
    print("=" * 60)

    try:
        data = fetch_fedwatch_nearest()

        print("\n🔍 3. 수집 결과")
        print("-" * 60)
        print(f"  회의일: {data['meetingDate']} | 현재 목표금리: {data['currentTargetRate']}")
        print(f"  기준: {data['dataAsOf']}")
        print(f"  요약  → 인하 {data['summary']['ease']}% / "
              f"동결 {data['summary']['noChange']}% / 인상 {data['summary']['hike']}%")
        for p in data["probabilities"]:
            tag = " (현재)" if p["isCurrent"] else ""
            print(f"    - {p['targetRate']}{tag}: {p['probability']}%")
        print("-" * 60)

        payload = {
            "crawlDate": crawl_date,
            "crawlTimestamp": firestore.SERVER_TIMESTAMP,
            "source": "CME FedWatch",
            "meetingDate": data["meetingDate"],
            "currentTargetRate": data["currentTargetRate"],
            "dataAsOf": data["dataAsOf"],
            "probabilities": data["probabilities"],
        }

        print("\n🔥 4. 파이어베이스 Firestore 동기화")
        print("=" * 60)
        deleted_count = 0
        if fedwatch_ref:
            # 문서 ID 를 KST 날짜로 고정 → 같은 날 재실행 시 덮어쓰기(멱등)
            fedwatch_ref.document(crawl_date).set(payload)
            print(f"✅  [저장 완료] 문서 ID: {crawl_date} | 오늘자 스냅샷을 기록했습니다.")
            deleted_count = prune_old_snapshots(crawl_date)
        else:
            print(f"📝 [드라이런] 문서 ID: {crawl_date} | (실제 저장 생략)")

        if logs_ref:
            logs_ref.add({
                "timestamp": firestore.SERVER_TIMESTAMP,
                "status": "SUCCESS",
                "task_name": TASK_NAME,
                "added_count": 1,
                "skipped_count": 0,
                "message": (f"FedWatch 스냅샷 저장 완료 - 회의일: {data['meetingDate']}, "
                            f"현재밴드: {data['currentTargetRate']}, "
                            f"동결 {data['summary']['noChange']}% / 인상 {data['summary']['hike']}%, "
                            f"만료삭제: {deleted_count}건")
            })

        print("\n" + "=" * 60)
        print(f"🏁 파이프라인 종료. 저장: 1건 / 만료삭제: {deleted_count}건")
        print("=" * 60 + "\n")

    except Exception as e:
        error_msg = str(e)
        print(f"\n❌ [에러 발생 및 중단] : {error_msg}")
        if logs_ref:
            logs_ref.add({
                "timestamp": firestore.SERVER_TIMESTAMP,
                "status": "FAILED",
                "task_name": TASK_NAME,
                "added_count": 0,
                "skipped_count": 0,
                "message": f"FedWatch 크롤러 실패 에러 로그: {error_msg}"
            })


if __name__ == "__main__":
    # 한국시간(KST) 매일 04시 1회 실행.
    # GitHub Actions cron 은 UTC 기준이므로 '0 19 * * *'(19:00 UTC = 익일 04:00 KST)로 설정함.
    # 매일 실행이라 별도 날짜 가드는 불필요.
    # 실행 시 이 배치 자신의 다음 실행 예정시간만 Firestore(crawler_schedules)에 기록
    update_my_schedule(db, __file__, display_name=TASK_NAME)

    run_fedwatch_crawler()
