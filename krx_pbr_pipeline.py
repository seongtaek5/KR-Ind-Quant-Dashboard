"""
KRX 산업별 PBR 크롤러 & 파이프라인
=====================================
사용법:
  pip install requests pandas
  python krx_pbr_pipeline.py

[세션 만료 대응]
  - 수집 중 JSESSIONID 만료되면 → 그때까지 수집한 데이터를 CSV에 저장 후 종료
  - 다시 실행하면 → CSV 마지막 날짜부터 자동으로 이어받기
  - 50건마다 중간 저장 → 만료돼도 최대 50건치 손실
  - 즉, 새 쿠키로 재실행만 반복하면 결국 전체 수집 완료
"""

import time
import sys
import os
import random
import requests
import pandas as pd
from datetime import datetime, timedelta


# ==============================================================================
# 설정
# ==============================================================================
SECTOR_MAP = {
    "KRX 자동차":    "KRX 자동차",
    "KRX 반도체":    "KRX 반도체",
    "KRX 헬스케어":  "KRX 헬스케어",
    "KRX 은행":      "KRX 은행",
    "KRX 에너지화학":"KRX 에너지화학",
    "KRX 철강":      "KRX 철강",
    "KRX 방송통신":  "KRX 방송통신",
    "KRX 건설":      "KRX 건설",
    "KRX 증권":      "KRX 증권",
    "KRX 기계장비":  "KRX 기계장비",
    "KRX 보험":      "KRX 보험",
    "KRX 운송":      "KRX 운송",
    "KRX 경기소비재":"KRX 경기소비재",
    "KRX 필수소비재":"KRX 필수소비재",
    "KRX K콘텐츠":   "KRX K콘텐츠",
    "KRX 정보기술":  "KRX 정보기술",
    "KRX 유틸리티":  "KRX 유틸리티",
}

TARGET_NAMES   = set(SECTOR_MAP.values())
OUTPUT_CSV     = "krx_sector_pbr.csv"
KRX_URL        = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
SAVE_EVERY     = 50    # N건마다 중간 저장
DELAY_MIN      = 1.5   # 요청 간 최소 대기(초)
DELAY_MAX      = 3.0   # 요청 간 최대 대기(초)


# ==============================================================================
# 헬퍼
# ==============================================================================

def make_headers(cookie: str) -> dict:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Origin":            "https://data.krx.co.kr",
        "Referer":           "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010107",
        "Content-Type":      "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept":            "application/json, text/javascript, */*; q=0.01",
        "Accept-Language":   "ko-KR,ko;q=0.9",
        "Accept-Encoding":   "gzip, deflate, br",
        "X-Requested-With":  "XMLHttpRequest",
        "Connection":        "keep-alive",
        "Sec-Fetch-Site":    "same-origin",
        "Sec-Fetch-Mode":    "cors",
        "Sec-Fetch-Dest":    "empty",
        "Cookie":            cookie,
    }


def _get_weekdays(fromdate: str, todate: str) -> list:
    start = datetime.strptime(fromdate, "%Y%m%d")
    end   = datetime.strptime(todate,   "%Y%m%d")
    days, cur = [], start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return days


def _last_weekday(date_str: str) -> str:
    d = datetime.strptime(date_str, "%Y%m%d")
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


# ==============================================================================
# CSV 중간 저장 (만료 대비)
# ==============================================================================

def _save_progress(records: list, existing: pd.DataFrame) -> None:
    """수집된 records를 existing에 병합해서 CSV에 저장."""
    if not records:
        return

    new_df = pd.DataFrame(records).set_index("날짜")
    for col in SECTOR_MAP.values():
        if col not in new_df.columns:
            new_df[col] = float("nan")
    new_df = new_df[list(SECTOR_MAP.values())]

    if not existing.empty:
        combined = pd.concat([existing, new_df])
        combined = combined[~combined.index.duplicated(keep="last")]
        combined.sort_index(inplace=True)
    else:
        combined = new_df.sort_index()

    combined.index = pd.to_datetime(combined.index).strftime("%Y-%m-%d")
    combined.index.name = "날짜"
    combined.to_csv(OUTPUT_CSV, encoding="utf-8-sig")


# ==============================================================================
# 단일 날짜 PBR 조회
# ==============================================================================

def fetch_pbr_single(date: str, headers: dict) -> dict:
    """
    반환값:
      - 정상: {"KRX 자동차": 0.82, ...}
      - 휴장일: {}
      - 만료: SessionExpiredError raise
    """
    payload = {
        "bld":             "dbms/MDC/STAT/standard/MDCSTAT00701",
        "locale":          "ko_KR",
        "searchType":      "A",
        "idxIndMidclssCd": "01",
        "trdDd":           date,
        "tboxindTpCd_finder_equidx0_0": "",
        "indTpCd":         "",
        "indTpCd2":        "",
        "codeNmindTpCd_finder_equidx0_0": "",
        "param1indTpCd_finder_equidx0_0": "",
        "strtDd":          "",
        "endDd":           "",
        "csvxls_isNo":     "false",
    }

    resp = requests.post(KRX_URL, headers=headers, data=payload, timeout=20)

    if resp.text.strip() == "LOGOUT":
        raise SessionExpiredError()

    if resp.status_code == 403:
        raise SessionExpiredError()

    resp.raise_for_status()
    data   = resp.json()
    output = data.get("output", [])
    if not output:
        return {}

    df = pd.DataFrame(output)
    pbr_vals = (
        df["WT_STKPRC_NETASST_RTO"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .replace({"-": "0", "": "0"})
    )
    if (pbr_vals == "0").all():
        return {}

    result = {}
    for _, row in df.iterrows():
        name = row.get("IDX_NM", "")
        if name not in TARGET_NAMES:
            continue
        raw = str(row.get("WT_STKPRC_NETASST_RTO", "0")).replace(",", "").strip()
        if raw in ("-", "", "0"):
            result[name] = float("nan")
        else:
            try:
                result[name] = float(raw)
            except ValueError:
                result[name] = float("nan")

    return result


class SessionExpiredError(Exception):
    pass


# ==============================================================================
# 기간 수집 (만료 시 중간 저장 후 종료)
# ==============================================================================

def fetch_pbr_range(
    fromdate: str,
    todate: str,
    headers: dict,
    existing: pd.DataFrame,
) -> pd.DataFrame:
    """
    수집 중 세션 만료 → 그때까지 수집한 데이터 저장 후 종료.
    다음 실행 시 저장된 CSV의 마지막 날짜부터 이어받음.
    """
    weekdays = _get_weekdays(fromdate, todate)
    total    = len(weekdays)
    print(f"\n  수집 시작: {total}개 영업일 ({fromdate} ~ {todate})")
    print(f"  딜레이: {DELAY_MIN}~{DELAY_MAX}초 랜덤 / {SAVE_EVERY}건마다 중간 저장\n")

    records = []
    skipped = 0

    for i, date in enumerate(weekdays):

        try:
            row_data = fetch_pbr_single(date, headers)

        except SessionExpiredError:
            # 만료 → 지금까지 수집한 것 저장 후 안내
            print(f"\n  [세션 만료] {date} 에서 중단")
            print(f"  지금까지 수집한 {len(records)}건을 CSV에 저장합니다...")
            _save_progress(records, existing)
            print(f"\n  CSV 저장 완료: {OUTPUT_CSV}")
            print(f"  다음 실행 시 이 날짜({date})부터 자동으로 이어받습니다.")
            print("\n  [할 일] KRX에서 재로그인 → 새 쿠키 복사 → 스크립트 재실행")

            # CI/비대화형 또는 strict 모드에서는 실패로 처리해
            # 데이터 미수집 상태를 성공으로 오인하지 않게 한다.
            strict = os.getenv("KRX_STRICT", "").strip() == "1"
            if strict or (not sys.stdin.isatty()):
                print("  [오류] 세션 만료로 수집이 중단되었습니다. 종료코드 1로 종료합니다.")
                sys.exit(1)

            sys.exit(0)

        except KeyboardInterrupt:
            print(f"\n  [중단] 지금까지 수집한 {len(records)}건 저장 중...")
            _save_progress(records, existing)
            print(f"  저장 완료. 재실행 시 이어받습니다.")
            sys.exit(0)

        except Exception as e:
            print(f"  [{date}] 요청 실패 (스킵): {e}")
            skipped += 1
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            continue

        if not row_data:
            skipped += 1
        else:
            row_data["날짜"] = pd.to_datetime(date, format="%Y%m%d")
            records.append(row_data)

        # 진행 표시 & 중간 저장
        if (i + 1) % SAVE_EVERY == 0:
            pct = (i + 1) / total * 100
            print(
                f"  진행 {i+1:>5}/{total} ({pct:5.1f}%) | "
                f"수집: {len(records)}건 | 스킵: {skipped}건 | [중간 저장 중...]"
            )
            _save_progress(records, existing)
            # 중간 저장 후 existing 갱신 (다음 저장 때 중복 방지)
            if os.path.exists(OUTPUT_CSV):
                existing = pd.read_csv(OUTPUT_CSV, index_col="날짜", parse_dates=True)
                for col in SECTOR_MAP.values():
                    if col not in existing.columns:
                        existing[col] = float("nan")
                existing = existing[list(SECTOR_MAP.values())]
            records = []  # 저장된 것은 메모리에서 비움

        elif (i + 1) == total:
            pct = (i + 1) / total * 100
            print(
                f"  진행 {i+1:>5}/{total} ({pct:5.1f}%) | "
                f"수집: {len(records)}건 | 스킵: {skipped}건"
            )

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # 마지막 남은 records 저장
    _save_progress(records, existing)

    # 최종 CSV 읽어서 반환
    if os.path.exists(OUTPUT_CSV):
        result = pd.read_csv(OUTPUT_CSV, index_col="날짜", parse_dates=True)
        for col in SECTOR_MAP.values():
            if col not in result.columns:
                result[col] = float("nan")
        return result[list(SECTOR_MAP.values())]
    return pd.DataFrame()


# ==============================================================================
# 메인
# ==============================================================================

def main():
    print("=" * 65)
    print("   KRX 산업별 PBR 데이터 파이프라인")
    print("=" * 65)

    # 쿠키 입력 (비대화형 실행 시 환경변수 우선)
    cookie = os.getenv("KRX_COOKIE", "").strip()
    if cookie:
        print("\n[1/2] KRX_COOKIE 환경변수에서 쿠키를 읽었습니다.")
    else:
        if not sys.stdin.isatty():
            print("[오류] 비대화형 실행에서는 KRX_COOKIE 환경변수가 필요합니다.")
            sys.exit(1)
        print("\n[1/2] KRX 쿠키를 붙여넣으세요.")
        print("      (KRX 조회 실행 후 F12 → Network → getJsonData.cmd")
        print("       → Request Headers → Cookie 전체 복사)")
        cookie = input("\nCOOKIE > ").strip()
    if not cookie:
        print("[오류] 쿠키가 비어있습니다.")
        sys.exit(1)

    # 날짜 입력 (비대화형 실행 시 환경변수 우선)
    default_today = datetime.today().strftime("%Y%m%d")
    raw = os.getenv("KRX_TODAY", "").strip()
    if raw:
        try:
            datetime.strptime(raw, "%Y%m%d")
            today_str = raw
            print(f"\n[2/2] KRX_TODAY 환경변수 사용: {today_str}")
        except ValueError:
            print("[오류] KRX_TODAY는 YYYYMMDD 형식이어야 합니다.")
            sys.exit(1)
    else:
        if not sys.stdin.isatty():
            today_str = default_today
            print(f"\n[2/2] 비대화형 실행: 기본 날짜 {today_str} 사용")
        else:
            print(f"\n[2/2] 오늘 날짜 (YYYYMMDD, Enter = {default_today}):")
            raw = input("날짜   > ").strip()
            if not raw:
                today_str = default_today
            else:
                try:
                    datetime.strptime(raw, "%Y%m%d")
                    today_str = raw
                except ValueError:
                    print("[오류] YYYYMMDD 형식으로 입력하세요.")
                    sys.exit(1)

    print(f"\n  수집 기준일: {today_str}\n")

    headers = make_headers(cookie)

    # 기존 CSV 확인 → 수집 시작점 결정
    if os.path.exists(OUTPUT_CSV):
        existing = pd.read_csv(OUTPUT_CSV, index_col="날짜", parse_dates=True)
        for col in SECTOR_MAP.values():
            if col not in existing.columns:
                existing[col] = float("nan")
        existing = existing[list(SECTOR_MAP.values())]

        last_date  = existing.index.max()
        fetch_from = (last_date - timedelta(days=30)).strftime("%Y%m%d")

        print(f"[기존 CSV] 마지막 날짜 = {last_date.date()}")
        print(f"  → {fetch_from} 부터 이어받습니다.\n")
    else:
        existing   = pd.DataFrame()
        fetch_from = "20100101"
        total_days = len(_get_weekdays(fetch_from, today_str))
        est_hours  = total_days * 2.25 / 3600  # 평균 2.25초 기준
        print(f"[신규] 2010-01-01부터 전체 수집합니다.")
        print(f"  대상: 약 {total_days}개 영업일")
        print(f"  예상 시간: 약 {est_hours:.1f}시간 (세션 만료 시 재실행 필요)")
        print(f"  세션 만료되면 자동 저장 후 종료 → 재실행하면 이어받기\n")

    # 수집 실행
    final_df = fetch_pbr_range(fetch_from, today_str, headers, existing)

    if final_df is not None and not final_df.empty:
        print(f"\n[완료] {OUTPUT_CSV}")
        print(f"  기간: {final_df.index.min()} ~ {final_df.index.max()}")
        print(f"  총 {len(final_df)}개 날짜\n")
        print("[최근 5거래일]")
        print(final_df.tail(5).to_string())


if __name__ == "__main__":
    main()
