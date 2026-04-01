"""pykrx 기반 산업 ETF/코스피 일별 데이터 수집 스크립트."""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import pandas as pd
from pykrx import stock

SECTOR_OUTPUT_CSV = "kodex_sector_etf_close.csv"
KOSPI_OUTPUT_CSV = "kospi_daily.csv"

# 대시보드에서 사용하는 섹터 컬럼명 기준
SECTOR_KEYWORDS = {
    "자동차": ["자동차"],
    "반도체": ["반도체"],
    "헬스케어": ["헬스케어"],
    "은행": ["은행"],
    "에너지화학": ["에너지화학"],
    "철강": ["철강"],
    "건설": ["건설"],
    "증권": ["증권"],
    "기계장비": ["기계장비"],
    "보험": ["보험"],
    "운송": ["운송"],
    "경기소비재": ["경기소비재"],
    "필수소비재": ["필수소비재"],
    "K콘텐츠": ["K콘텐츠", "콘텐츠"],
    "IT": ["IT", "정보기술"],
}


def _validate_yyyymmdd(date_str: str) -> str:
    datetime.strptime(date_str, "%Y%m%d")
    return date_str


def _resolve_today() -> str:
    env_today = os.getenv("KRX_TODAY", "").strip()
    if env_today:
        return _validate_yyyymmdd(env_today)
    return datetime.today().strftime("%Y%m%d")


def _compute_fetch_start(existing_csv: str, date_col: str, default_start: str) -> str:
    if not os.path.exists(existing_csv):
        return default_start
    try:
        existing = pd.read_csv(existing_csv)
        last_date = pd.to_datetime(existing[date_col]).max()
        # 최근 구간을 조금 덮어써서 누락/정정 데이터를 보완
        restart_from = (last_date - timedelta(days=7)).strftime("%Y%m%d")
        return restart_from
    except Exception:
        return default_start


def _choose_best_ticker(candidates: list[tuple[str, str]], preferred_keywords: list[str]) -> str | None:
    if not candidates:
        return None

    # 1순위: 완전 일치에 가까운 이름
    for keyword in preferred_keywords:
        exact = [t for t, name in candidates if name == f"KODEX {keyword}"]
        if exact:
            return exact[0]

    # 2순위: 이름이 짧은 상품 우선
    candidates_sorted = sorted(candidates, key=lambda x: len(x[1]))
    return candidates_sorted[0][0]


def build_sector_ticker_map(asof: str) -> dict[str, str]:
    tickers = stock.get_etf_ticker_list(asof)
    ticker_name_pairs = [(ticker, stock.get_etf_ticker_name(ticker)) for ticker in tickers]

    sector_map: dict[str, str] = {}
    for sector, keywords in SECTOR_KEYWORDS.items():
        matched = []
        for ticker, name in ticker_name_pairs:
            if not name.startswith("KODEX"):
                continue
            if any(keyword in name for keyword in keywords):
                matched.append((ticker, name))

        selected = _choose_best_ticker(matched, keywords)
        if selected:
            sector_map[sector] = selected

    return sector_map


def collect_sector_close(fromdate: str, todate: str) -> pd.DataFrame:
    ticker_map = build_sector_ticker_map(todate)
    if not ticker_map:
        raise RuntimeError("KODEX 섹터 ETF 티커를 찾지 못했습니다.")

    merged: pd.DataFrame | None = None

    for sector, ticker in ticker_map.items():
        df = stock.get_etf_ohlcv_by_date(fromdate, todate, ticker)
        if df.empty or "종가" not in df.columns:
            continue

        series = df[["종가"]].copy()
        series.columns = [sector]

        if merged is None:
            merged = series
        else:
            merged = merged.join(series, how="outer")

    if merged is None or merged.empty:
        raise RuntimeError("섹터 ETF 종가 데이터를 수집하지 못했습니다.")

    merged.index = pd.to_datetime(merged.index)
    merged.sort_index(inplace=True)
    merged.index.name = "날짜"
    merged = merged.reset_index()

    if os.path.exists(SECTOR_OUTPUT_CSV):
        existing = pd.read_csv(SECTOR_OUTPUT_CSV, parse_dates=["날짜"])
        combined = pd.concat([existing, merged], ignore_index=True)
        combined.sort_values("날짜", inplace=True)
        combined.drop_duplicates(subset=["날짜"], keep="last", inplace=True)
    else:
        combined = merged

    combined["날짜"] = pd.to_datetime(combined["날짜"]).dt.strftime("%Y-%m-%d")
    ordered_cols = ["날짜"] + [c for c in SECTOR_KEYWORDS.keys() if c in combined.columns]
    combined = combined[ordered_cols]
    combined.to_csv(SECTOR_OUTPUT_CSV, index=False, encoding="utf-8-sig")
    return combined


def collect_kospi_ohlcv(fromdate: str, todate: str) -> pd.DataFrame:
    # 코스피 지수 코드: 1001
    df = stock.get_index_ohlcv_by_date(fromdate, todate, "1001")
    if df.empty:
        raise RuntimeError("코스피 지수 데이터를 수집하지 못했습니다.")

    out = df.rename(
        columns={
            "시가": "Open",
            "고가": "High",
            "저가": "Low",
            "종가": "Close",
            "거래량": "Volume",
        }
    )[["Open", "High", "Low", "Close", "Volume"]].copy()

    out["Adj Close"] = out["Close"]
    out = out[["Open", "High", "Low", "Close", "Adj Close", "Volume"]]

    out.index = pd.to_datetime(out.index)
    out.index.name = "date"
    out = out.reset_index()

    if os.path.exists(KOSPI_OUTPUT_CSV):
        existing = pd.read_csv(KOSPI_OUTPUT_CSV, parse_dates=["date"])
        combined = pd.concat([existing, out], ignore_index=True)
        combined.sort_values("date", inplace=True)
        combined.drop_duplicates(subset=["date"], keep="last", inplace=True)
    else:
        combined = out

    combined["date"] = pd.to_datetime(combined["date"]).dt.strftime("%Y-%m-%d")
    combined.to_csv(KOSPI_OUTPUT_CSV, index=False)
    return combined


def main() -> None:
    todate = _resolve_today()
    sector_fromdate = _compute_fetch_start(SECTOR_OUTPUT_CSV, "날짜", default_start="20100101")
    kospi_fromdate = _compute_fetch_start(KOSPI_OUTPUT_CSV, "date", default_start="19900101")

    print(f"[ETF] 수집 구간: {sector_fromdate} ~ {todate}")
    sector_df = collect_sector_close(sector_fromdate, todate)
    print(
        f"[ETF] 저장 완료: {SECTOR_OUTPUT_CSV} | "
        f"{sector_df['날짜'].min()} ~ {sector_df['날짜'].max()} | {len(sector_df)}행"
    )

    print(f"[KOSPI] 수집 구간: {kospi_fromdate} ~ {todate}")
    kospi_df = collect_kospi_ohlcv(kospi_fromdate, todate)
    print(
        f"[KOSPI] 저장 완료: {KOSPI_OUTPUT_CSV} | "
        f"{kospi_df['date'].min()} ~ {kospi_df['date'].max()} | {len(kospi_df)}행"
    )


if __name__ == "__main__":
    main()
