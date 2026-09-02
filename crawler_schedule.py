"""
크롤러 실행 일정(다음 실행 예정시간) 기록 유틸리티.

각 크롤러가 실행될 때 update_my_schedule(db, __file__) 를 호출하면,
그 크롤러 '자신'의 워크플로우 yml 에서 cron 을 읽어 다음 실행 예정시간을
계산해 Firestore 의 crawler_schedules 컬렉션에 자기 문서 1건만 기록한다.

설계 원칙:
- 자기 것만 기록 → 한 배치가 실행될 때 다른 배치 일정까지 들여다보거나
  갱신하지 않는다. DB 접근은 자기 문서 읽기 1회 + (변경 시) 쓰기 1회로 최소.
- 값이 바뀌지 않았으면 쓰기를 생략한다.
- 새 크롤러는 그 크롤러가 처음 실행되는 시점에 문서가 생성된다.
  (전체 스캔을 하지 않으므로, 아직 한 번도 안 돈 크롤러는 표에 없음)
- 외부 의존성 없이 동작하도록 cron 다음 실행시각 계산기를 자체 구현했다.
"""

import os
import re
import glob
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WORKFLOW_DIR = os.path.join(BASE_DIR, ".github", "workflows")
COLLECTION = "crawler_schedules"

# 각 cron 필드의 허용 범위 (분 시 일 월 요일)
FIELD_RANGES = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 6)]

# 저장값과 계산값을 비교해 '변경 여부'를 판단할 필드
COMPARE_FIELDS = ("crawler", "workflowFile", "cron", "allCrons",
                  "kstDay1Guard", "nextRunUtc", "nextRunKst")


def _parse_field(field, lo, hi):
    """cron 단일 필드('*', '5', '1-5', '*/2', '1,3,28-31')를 허용 정수 집합으로 변환"""
    if field == "*":
        return set(range(lo, hi + 1))
    values = set()
    for part in field.split(","):
        step = 1
        rng = part
        if "/" in part:
            rng, step_str = part.split("/")
            step = int(step_str)
        if rng == "*":
            a, b = lo, hi
        elif "-" in rng:
            a_str, b_str = rng.split("-")
            a, b = int(a_str), int(b_str)
        else:
            a = b = int(rng)
        values.update(range(a, b + 1, step))
    return values


def _parse_cron(expr):
    """'분 시 일 월 요일' → 필드별 집합 + 일/요일이 '*'인지 여부"""
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError(f"지원하지 않는 cron 형식: {expr!r}")
    sets = [_parse_field(p, lo, hi) for p, (lo, hi) in zip(parts, FIELD_RANGES)]
    dom_is_star = parts[2] == "*"
    dow_is_star = parts[4] == "*"
    return sets, dom_is_star, dow_is_star


def _next_fire(expr, guard_kst_day1, base_utc):
    """base_utc(UTC) 이후 이 cron 이 실제로 '동작'하는 다음 UTC 시각을 계산.

    guard_kst_day1=True 이면 (발화시각 + 9h) 의 날짜가 1일인 경우만 유효(월간 배치 가드).
    """
    (mi, ho, dom, mo, dow), dom_star, dow_star = _parse_cron(expr)
    start = base_utc.replace(second=0, microsecond=0) + timedelta(minutes=1)

    for dday in range(0, 400):  # 최대 약 13개월 앞까지 탐색
        d = base_utc.date() + timedelta(days=dday)
        if d.month not in mo:
            continue
        dom_ok = d.day in dom
        dow_cron = (d.weekday() + 1) % 7  # 파이썬(월=0) → cron(일=0)
        dow_ok = dow_cron in dow
        if dom_star and dow_star:
            day_ok = True
        elif dom_star:
            day_ok = dow_ok
        elif dow_star:
            day_ok = dom_ok
        else:  # 표준 cron: 일/요일 모두 지정되면 OR
            day_ok = dom_ok or dow_ok
        if not day_ok:
            continue
        for H in sorted(ho):
            for M in sorted(mi):
                cand = datetime(d.year, d.month, d.day, H, M)
                if cand < start:
                    continue
                if guard_kst_day1 and (cand + timedelta(hours=9)).day != 1:
                    continue
                return cand
    return None


def _read_workflow(path):
    """워크플로우 yml 에서 cron 목록과 실행 스크립트명을 추출"""
    with open(path, encoding="utf-8") as f:
        text = f.read()
    crons = re.findall(r"cron:\s*['\"]([^'\"]+)['\"]", text)
    m = re.search(r"python\s+([A-Za-z0-9_./-]+\.py)", text)
    script = m.group(1) if m else None
    return crons, script


def _find_workflow_for(script_basename):
    """해당 스크립트를 실행하는 워크플로우 yml 경로와 cron 목록을 찾는다"""
    for path in sorted(glob.glob(os.path.join(WORKFLOW_DIR, "*.yml"))
                       + glob.glob(os.path.join(WORKFLOW_DIR, "*.yaml"))):
        crons, script = _read_workflow(path)
        if script and os.path.basename(script) == script_basename:
            return path, crons
    return None, None


def _has_kst_day1_guard(script_basename):
    """해당 크롤러 스크립트에 'KST 1일에만 실행' 가드(.day != 1)가 있는지 확인"""
    path = os.path.join(BASE_DIR, script_basename)
    if not os.path.exists(path):
        return False
    with open(path, encoding="utf-8") as f:
        src = f.read()
    return re.search(r"\.day\s*!=\s*1", src) is not None


def compute_schedule_for(script_file, base_utc=None):
    """주어진 크롤러 스크립트 '자신'의 다음 실행 예정시간을 계산해 dict 반환(없으면 None)"""
    base_utc = base_utc or datetime.utcnow()
    script_basename = os.path.basename(script_file)
    path, crons = _find_workflow_for(script_basename)
    if not path or not crons:
        return None  # 스케줄(cron)이 없는 스크립트는 기록 대상 아님
    guard = _has_kst_day1_guard(script_basename)
    next_utc, next_cron = None, None
    for expr in crons:
        try:
            fire = _next_fire(expr, guard, base_utc)
        except ValueError:
            continue
        if fire and (next_utc is None or fire < next_utc):
            next_utc, next_cron = fire, expr
    if next_utc is None:
        return None
    next_kst = next_utc + timedelta(hours=9)
    return {
        "key": os.path.splitext(script_basename)[0],
        "crawler": script_basename,
        "workflowFile": os.path.relpath(path, BASE_DIR).replace("\\", "/"),
        "cron": next_cron,
        "allCrons": crons,
        "kstDay1Guard": guard,
        "nextRunUtc": next_utc.strftime("%Y-%m-%d %H:%M") + " UTC",
        "nextRunKst": next_kst.strftime("%Y-%m-%d %H:%M") + " KST",
    }


def update_my_schedule(db, script_file, verbose=True):
    """이 크롤러 '자신'의 다음 실행 예정시간을 crawler_schedules 에 기록.

    - 자기 문서만 다룬다(다른 배치는 건드리지 않음).
    - 값이 바뀌지 않았으면 쓰기를 생략한다(읽기 1회만).
    - db 가 None 이면 드라이런(출력만).
    - 어떤 예외도 밖으로 던지지 않는다(일정 기록 실패가 본 크롤링을 중단시키지 않도록).
    """
    try:
        s = compute_schedule_for(script_file)
        if s is None:
            if verbose:
                print("🗓️  (일정 정보 없음 - 워크플로우/cron 미발견, 기록 생략)")
            return None
        if verbose:
            print(f"🗓️  다음 실행 예정: {s['nextRunKst']}  [{s['key']}]")

        if db is None:
            if verbose:
                print("📝 [드라이런] crawler_schedules 저장 생략")
            return s

        try:
            from google.cloud import firestore
            server_ts = firestore.SERVER_TIMESTAMP
        except Exception:
            server_ts = None

        ref = db.collection(COLLECTION).document(s["key"])
        snap = ref.get()
        prev = snap.to_dict() if snap.exists else None
        if prev and all(prev.get(f) == s.get(f) for f in COMPARE_FIELDS):
            if verbose:
                print("✅ 일정 변경 없음 → 저장 생략")
            return s

        payload = dict(s)
        payload["updatedAt"] = server_ts
        ref.set(payload)
        if verbose:
            print("✅ 다음 실행 예정시간 기록 완료")
        return s
    except Exception as e:
        print(f"⚠️ [경고] 일정 기록 중 오류(본 작업에는 영향 없음): {e}")
        return None


def compute_all(base_utc=None):
    """(로컬 점검용) 전체 워크플로우의 다음 실행 예정시간을 계산해 리스트로 반환.

    런타임 크롤러에서는 사용하지 않는다 - 개발자가 로컬에서 전체 일정을
    한눈에 확인할 때만 쓴다(DB 접근 없음, 로컬 파일만 읽음).
    """
    base_utc = base_utc or datetime.utcnow()
    results = []
    for path in sorted(glob.glob(os.path.join(WORKFLOW_DIR, "*.yml"))
                       + glob.glob(os.path.join(WORKFLOW_DIR, "*.yaml"))):
        _, script = _read_workflow(path)
        if not script:
            continue
        s = compute_schedule_for(script, base_utc)
        if s:
            results.append(s)
    return results


if __name__ == "__main__":
    # 로컬 단독 실행 시: Firestore 없이 전체 일정 계산 결과만 출력(점검용)
    print("🗓️  전체 크롤러 다음 실행 예정시간(로컬 계산)")
    print("-" * 60)
    for s in compute_all():
        print(f"  {s['key']:35} {s['nextRunKst']}")
    print("-" * 60)
