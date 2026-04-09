"""yfinance 기반 산업 ETF / 코스피 일별 데이터 수집 스크립트.

사용법:
    python price_data_collect.py

- 기존 CSV가 없으면 전체 기간(2010-01-01~오늘)을 수집
- 기존 CSV가 있으면 마지막 날짜 이후 빈 구간만 채워서 저장
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

# CSV 파일은 항상 이 스크립트와 같은 디렉터리에 저장
_DIR = Path(__file__).parent
SECTOR_CSV = _DIR / "kodex_sector_etf_close.csv"
KOSPI_CSV  = _DIR / "kospi_daily.csv"

KODEX_SECTOR_TICKERS = {
    "자동차":     "091180.KS",
    "반도체":     "091160.KS",
    "헬스케어":   "266420.KS",
    "은행":       "091170.KS",
    "에너지화학": "117460.KS",
    "철강":       "117680.KS",
    "건설":       "117700.KS",
    "증권":       "102970.KS",
    "기계장비":   "102960.KS",
    "보험":       "140700.KS",
    "운송":       "140710.KS",
    "경기소비재": "266390.KS",
    "필수소비재": "266410.KS",
    "IT":         "266370.KS",
    "K콘텐츠":   "266360.KS",
}


def _last_date_in_csv(csv_path: Path, date_col: str) -> pd.Timestamp | None:
    """CSV에서 가장 최근 날짜를 반환. 파일이 없거나 읽기 실패 시 None."""
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path, usecols=[date_col])
        return pd.to_datetime(df[date_col]).max()
    except Exception:
        return None


def _fetch_start(last_date: pd.Timestamp | None, default_start: str) -> str:
    """마지막 날짜 기준 하루 뒤부터 수집 (없으면 default_start 사용)."""
    if last_date is None:
        return default_start
    return (last_date + timedelta(days=1)).strftime("%Y-%m-%d")


def collect_sector_close(fromdate: str, todate: str) -> pd.DataFrame:
    """fromdate~todate 구간 ETF 종가를 수집해 CSV에 추가 저장."""
    merged: pd.DataFrame | None = None

    for sector, ticker in KODEX_SECTOR_TICKERS.items():
        print(f"  [{sector}] 수집 중... ({ticker})")
        try:
            df = yf.download(ticker, start=fromdate, end=todate, progress=False)
            if df.empty:
                print(f"  [{sector}] 데이터 없음, 건너뜀")
                continue

            close = df["Close"]
            if isinstance(close, pd.DataFrame):
                close = close.iloc[:, 0]
            frame = close.to_frame(name=sector)
            merged = frame if merged is None else merged.join(frame, how="outer")
        except Exception as e:
            print(f"  [{sector}] 오류: {e}")

    if merged is None or merged.empty:
        raise RuntimeError("섹터 ETF 종가 데이터를 수집하지 못했습니다.")

    merged.index = pd.to_datetime(merged.index)
    merged.sort_index(inplace=True)
    merged.index.name = "날짜"
    new_df = merged.reset_index()

    # 기존 CSV와 병합 (중복 날짜는 새 데이터 우선)
    if SECTOR_CSV.exists():
        existing = pd.read_csv(SECTOR_CSV, parse_dates=["날짜"])
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined.sort_values("날짜", inplace=True)
        combined.drop_duplicates(subset=["날짜"], keep="last", inplace=True)
    else:
        combined = new_df

    combined["날짜"] = pd.to_datetime(combined["날짜"]).dt.strftime("%Y-%m-%d")
    cols = ["날짜"] + [c for c in KODEX_SECTOR_TICKERS if c in combined.columns]
    combined = combined[cols]
    combined.to_csv(SECTOR_CSV, index=False, encoding="utf-8-sig")
    return combined


def collect_kospi_ohlcv(fromdate: str, todate: str) -> pd.DataFrame:
    """fromdate~todate 구간 KOSPI OHLCV를 수집해 CSV에 추가 저장."""
    df = yf.download("^KS11", start=fromdate, end=todate, progress=False)
    if df.empty:
        raise RuntimeError("코스피 지수 데이터를 수집하지 못했습니다.")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    out = df[["Open", "High", "Low", "Close", "Volume"]].copy()
    out["Adj Close"] = out["Close"]
    out.index = pd.to_datetime(out.index)
    out.index.name = "date"
    new_df = out.reset_index()

    if KOSPI_CSV.exists():
        existing = pd.read_csv(KOSPI_CSV, parse_dates=["date"])
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined.sort_values("date", inplace=True)
        combined.drop_duplicates(subset=["date"], keep="last", inplace=True)
    else:
        combined = new_df

    combined["date"] = pd.to_datetime(combined["date"]).dt.strftime("%Y-%m-%d")
    combined.to_csv(KOSPI_CSV, index=False)
    return combined


def main() -> None:
    today = datetime.today().strftime("%Y-%m-%d")
    # yfinance end는 exclusive이므로 내일 날짜를 전달해야 오늘 데이터까지 수집됨
    tomorrow = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")

    # ── ETF ──────────────────────────────────────────
    etf_last = _last_date_in_csv(SECTOR_CSV, "날짜")
    etf_from = _fetch_start(etf_last, default_start="2010-01-01")

    if etf_from > today:
        print(f"[ETF] 이미 최신 상태입니다. (마지막: {etf_last.date()})")
    else:
        print(f"[ETF] 수집 구간: {etf_from} ~ {today}")
        df = collect_sector_close(etf_from, tomorrow)
        print(
            f"[ETF] 저장 완료: {SECTOR_CSV.name} | "
            f"{df['날짜'].min()} ~ {df['날짜'].max()} | {len(df)}행"
        )

    # ── KOSPI ─────────────────────────────────────────
    kospi_last = _last_date_in_csv(KOSPI_CSV, "date")
    kospi_from = _fetch_start(kospi_last, default_start="1990-01-01")

    if kospi_from > today:
        print(f"[KOSPI] 이미 최신 상태입니다. (마지막: {kospi_last.date()})")
    else:
        print(f"[KOSPI] 수집 구간: {kospi_from} ~ {today}")
        df = collect_kospi_ohlcv(kospi_from, tomorrow)
        print(
            f"[KOSPI] 저장 완료: {KOSPI_CSV.name} | "
            f"{df['date'].min()} ~ {df['date'].max()} | {len(df)}행"
        )


if __name__ == "__main__":
    main()
