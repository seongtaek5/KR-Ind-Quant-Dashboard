"""산업별 Time-Series Z-Score 히트맵 Streamlit 앱 (로컬 CSV 기반)."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns
import streamlit as st


FONT_PROP: fm.FontProperties | None = None

# Streamlit Cloud에서 apt로 설치되는 NanumGothic 경로 후보
_NANUM_CANDIDATES = [
    "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
    "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf",
    "/usr/share/fonts/nanum/NanumGothic.ttf",
]

_FALLBACK_FONT_URL = (
    "https://raw.githubusercontent.com/google/fonts/main/ofl/notosanskr/"
    "NotoSansKR%5Bwght%5D.ttf"
)


def ensure_fallback_korean_font() -> Path | None:
    """다운로드 가능한 한글 폰트를 로컬 캐시에 저장해 fallback으로 사용한다."""
    font_dir = Path(__file__).parent / ".streamlit" / "fonts"
    target = font_dir / "NotoSansKR[wght].ttf"

    if target.exists() and target.stat().st_size > 0:
        return target

    try:
        font_dir.mkdir(parents=True, exist_ok=True)
        with urlopen(_FALLBACK_FONT_URL, timeout=10) as response:
            data = response.read()
        if not data:
            return None
        target.write_bytes(data)
        return target
    except (OSError, URLError, TimeoutError, ValueError):
        return None


def setup_korean_font() -> None:
    global FONT_PROP

    candidates = list(_NANUM_CANDIDATES)
    downloaded_font = ensure_fallback_korean_font()
    if downloaded_font is not None:
        candidates.insert(0, str(downloaded_font))

    # 1) 파일 경로로 직접 등록 (Streamlit Cloud 환경에서 가장 확실한 방법)
    for candidate in candidates:
        if Path(candidate).exists():
            fm.fontManager.addfont(candidate)
            FONT_PROP = fm.FontProperties(fname=candidate)
            plt.rcParams["font.family"] = "sans-serif"
            plt.rcParams["font.sans-serif"] = ["NanumGothic", "DejaVu Sans"]
            sns.set_theme(
                rc={
                    "font.family": "sans-serif",
                    "font.sans-serif": ["NanumGothic", "DejaVu Sans"],
                }
            )
            plt.rcParams["axes.unicode_minus"] = False
            return

    # 2) 폰트 캐시 갱신 후 이름으로 탐색 (로컬 개발 환경 fallback)
    try:
        fm._load_fontmanager(try_read_cache=False)
    except Exception:
        pass
    preferred = ["NanumGothic", "Noto Sans CJK KR", "Noto Sans KR", "Malgun Gothic"]
    available = {f.name for f in fm.fontManager.ttflist}
    selected = next((name for name in preferred if name in available), None)
    if selected:
        font_path = fm.findfont(selected, fallback_to_default=False)
        FONT_PROP = fm.FontProperties(fname=font_path)
        plt.rcParams["font.sans-serif"] = [selected, "DejaVu Sans"]
        plt.rcParams["font.family"] = "sans-serif"
        sns.set_theme(
            rc={
                "font.family": "sans-serif",
                "font.sans-serif": [selected, "DejaVu Sans"],
            }
        )
    plt.rcParams["axes.unicode_minus"] = False


setup_korean_font()

st.set_page_config(
    page_title="산업별 Z-Score 히트맵",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

BASE_DIR = Path(__file__).parent
PBR_PATH = BASE_DIR / "krx_sector_pbr.csv"
PRICE_PATH = BASE_DIR / "kodex_sector_etf_close.csv"
KOSPI_PATH = BASE_DIR / "kospi_daily.csv"
BENCHMARK_LABEL = "섹터평균"

SECTOR_ALIAS_MAP = {
    "IT": "정보기술",
    "K콘텐츠": "K콘텐츠",
}


def normalize_sector_name(name: str) -> str:
    s = str(name).strip()
    s = re.sub(r"^KODEX\s+", "", s)
    s = re.sub(r"^KRX\s+", "", s)
    s = re.sub(r"\s+", "", s)
    return SECTOR_ALIAS_MAP.get(s, s)


def prepare_aligned_data(pbr_df: pd.DataFrame, price_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    pbr_daily = (
        pbr_df.rename(columns={"날짜": "date"})
        .sort_values("date")
        .set_index("date")
        .rename(columns=lambda c: normalize_sector_name(c))
    )
    price_daily = (
        price_df.rename(columns={"날짜": "date"})
        .sort_values("date")
        .set_index("date")
        .rename(columns=lambda c: normalize_sector_name(c))
    )

    common_sectors = sorted(set(pbr_daily.columns).intersection(price_daily.columns))
    pbr_daily = pbr_daily[common_sectors].copy()
    price_daily = price_daily[common_sectors].copy()

    common_dates = pbr_daily.index.intersection(price_daily.index)
    pbr_daily = pbr_daily.loc[common_dates].sort_index()
    price_daily = price_daily.loc[common_dates].sort_index()
    return pbr_daily, price_daily


def rolling_zscore(data: pd.DataFrame, windows: list[int]) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    for window in windows:
        mean = data.rolling(window=window, min_periods=1).mean()
        std = data.rolling(window=window, min_periods=1).std()
        out[f"{window // 252}Y"] = (data - mean) / (std + 1e-10)
    return out


@st.cache_data(ttl=3600)
def load_local_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    pbr_df = pd.read_csv(PBR_PATH, parse_dates=["날짜"])
    price_df = pd.read_csv(PRICE_PATH, parse_dates=["날짜"])
    return pbr_df, price_df


@st.cache_data(ttl=3600)
def load_kospi_data() -> pd.Series:
    df = pd.read_csv(KOSPI_PATH, parse_dates=["date"])
    df = df.sort_values("date").set_index("date")
    return df["Close"].rename("KOSPI")


@st.cache_data(ttl=3600)
def build_heatmap_data(
    _pbr_df: pd.DataFrame,
    _price_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    pbr_daily, price_daily = prepare_aligned_data(_pbr_df, _price_df)

    pbr_z = rolling_zscore(pbr_daily, windows=[252])
    # 252일 누적 수익률(1Y MOM)을 매일 계산 후 1Y Z-Score
    mom_1y_ret = price_daily.pct_change(252) * 100
    mom_z = rolling_zscore(mom_1y_ret, windows=[252])

    # 한국 벤치마크: 섹터 1Y 누적 수익률의 단순평균
    benchmark_ret = price_daily.pct_change(252).mean(axis=1, skipna=True) * 100
    benchmark_z = rolling_zscore(benchmark_ret.to_frame(BENCHMARK_LABEL), windows=[252])

    max_date = pbr_daily.index.max()
    one_year_ago = max_date - pd.DateOffset(years=1)
    subset = pbr_daily.loc[pbr_daily.index >= one_year_ago]
    try:
        month_end_idx = (
            subset.groupby(subset.index.to_period("M"))
            .apply(lambda x: x.index.max(), include_groups=False)
        )
    except TypeError:
        month_end_idx = (
            subset.groupby(subset.index.to_period("M"))
            .apply(lambda x: x.index.max())
        )

    pbr_month = pbr_z["1Y"].loc[month_end_idx].copy()
    mom_month = mom_z["1Y"].loc[month_end_idx].copy()
    bench_month = benchmark_z["1Y"].loc[month_end_idx].copy()

    pbr_month[BENCHMARK_LABEL] = bench_month[BENCHMARK_LABEL].to_numpy()
    mom_month[BENCHMARK_LABEL] = bench_month[BENCHMARK_LABEL].to_numpy()

    return {"pbr_1Y": pbr_month, "mom_1Y": mom_month}


@st.cache_data(ttl=3600)
def build_timeseries_data(
    
    _pbr_df: pd.DataFrame,
    _price_df: pd.DataFrame,
) -> dict[str, pd.Series]:
    pbr_daily, price_daily = prepare_aligned_data(_pbr_df, _price_df)

    pbr_z_daily = rolling_zscore(pbr_daily, windows=[252])["1Y"]
    # 252일 누적 수익률(1Y MOM)을 매일 계산 후 1Y Z-Score
    mom_z_daily = rolling_zscore(price_daily.pct_change(252) * 100, windows=[252])["1Y"]

    # 섹터평균 벤치마크 컬럼은 cross-sectional 통계에서 제외
    pbr_sectors = [c for c in pbr_z_daily.columns if c != BENCHMARK_LABEL]
    mom_sectors = [c for c in mom_z_daily.columns if c != BENCHMARK_LABEL]

    pbr_mean = pbr_z_daily[pbr_sectors].mean(axis=1)
    pbr_std = pbr_z_daily[pbr_sectors].std(axis=1)
    mom_mean = mom_z_daily[mom_sectors].mean(axis=1)
    mom_std = mom_z_daily[mom_sectors].std(axis=1)

    # Std 시계열 자체의 1Y Rolling Z-Score
    pbr_std_zscore = rolling_zscore(pbr_std.to_frame("v"), windows=[252])["1Y"]["v"]
    mom_std_zscore = rolling_zscore(mom_std.to_frame("v"), windows=[252])["1Y"]["v"]

    return {
        "pbr_mean": pbr_mean,
        "pbr_std": pbr_std,
        "pbr_std_zscore": pbr_std_zscore,
        "mom_mean": mom_mean,
        "mom_std": mom_std,
        "mom_std_zscore": mom_std_zscore,
    }


def compute_shade_intervals(
    std_zscore: pd.Series,
    threshold: float = 1.65,
    bdays: int = 21,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """1Y Rolling Z-Score가 threshold를 초과한 날 이후 bdays 영업일을 음영 구간으로 반환."""
    trigger_dates = std_zscore[std_zscore > threshold].index

    shaded: set[pd.Timestamp] = set()
    for td in trigger_dates:
        future = pd.bdate_range(start=td, periods=bdays + 1)[1:]
        shaded.update(future)

    if not shaded:
        return []

    sorted_dates = sorted(shaded)
    intervals: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    start = sorted_dates[0]
    prev = sorted_dates[0]

    for d in sorted_dates[1:]:
        # 영업일 기준으로 연속 여부 확인 (3일 이하 gap 허용 → 주말 포함)
        if (d - prev).days <= 3:
            prev = d
        else:
            intervals.append((start, prev))
            start = d
            prev = d
    intervals.append((start, prev))
    return intervals


def render_heatmap(data: pd.DataFrame, title: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(11, 7))
    data_t = data.T
    date_labels = [d.strftime("%Y-%m") for d in data_t.columns]

    sns.heatmap(
        data_t,
        ax=ax,
        cmap="RdBu_r",
        center=0,
        cbar_kws={"label": "Z-Score"},
        xticklabels=date_labels,
        yticklabels=True,
        vmin=-2.5,
        vmax=2.5,
        linewidths=0.3,
        linecolor="lightgray",
        annot=True,
        fmt=".2f",
        annot_kws={"size": 9},
    )

    if FONT_PROP is not None:
        ax.set_title(title, fontsize=13, fontweight="bold", pad=12, fontproperties=FONT_PROP)
        ax.set_xlabel("Month-End Trading Day", fontsize=10, fontproperties=FONT_PROP)
        ax.set_ylabel("Sector", fontsize=10, fontproperties=FONT_PROP)
        for label in ax.get_xticklabels() + ax.get_yticklabels():
            label.set_fontproperties(FONT_PROP)
        for txt in ax.texts:
            txt.set_fontproperties(FONT_PROP)
    else:
        ax.set_title(title, fontsize=13, fontweight="bold", pad=12)
        ax.set_xlabel("Month-End Trading Day", fontsize=10)
        ax.set_ylabel("Sector", fontsize=10)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", fontsize=8)
    plt.setp(ax.get_yticklabels(), rotation=0, fontsize=8)
    plt.tight_layout()
    return fig


def render_timeseries(
    series: pd.Series,
    title: str,
    shade_intervals: list[tuple[pd.Timestamp, pd.Timestamp]] | None = None,
    ylabel: str = "Z-Score",
    ema_spans: list[int] | None = None,
) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(14, 4))

    line_alpha = 0.4 if ema_spans else 1.0
    ax.plot(series.index, series.values, linewidth=0.8, color="#2563eb", alpha=line_alpha, label="Daily")
    ax.axhline(0, color="gray", linestyle="--", linewidth=0.7)

    if ema_spans:
        ema_colors = {20: "#f59e0b", 60: "#10b981", 200: "#ef4444"}
        for span in ema_spans:
            ema = series.ewm(span=span, adjust=False).mean()
            color = ema_colors.get(span, "#6b7280")
            ax.plot(ema.index, ema.values, linewidth=1.3, color=color, label=f"EMA {span}")
        ax.legend(fontsize=8, loc="upper left")

    if shade_intervals:
        for start, end in shade_intervals:
            ax.axvspan(start, end, alpha=0.25, color="red", lw=0)

    ax.set_ylabel(ylabel, fontsize=10)
    ax.set_xlabel("")

    # x축 눈금: 연도별로 표시
    ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate(rotation=45, ha="right")

    if FONT_PROP is not None:
        ax.set_title(title, fontsize=12, fontweight="bold", pad=10, fontproperties=FONT_PROP)
        ax.set_ylabel(ylabel, fontsize=10, fontproperties=FONT_PROP)
        for label in ax.get_xticklabels() + ax.get_yticklabels():
            label.set_fontproperties(FONT_PROP)
    else:
        ax.set_title(title, fontsize=12, fontweight="bold", pad=10)

    plt.tight_layout()
    return fig


def render_combined_zscore_chart(
    mean_series: pd.Series,
    std_series: pd.Series,
    kospi: pd.Series,
    title: str,
    shade_intervals: list[tuple[pd.Timestamp, pd.Timestamp]] | None = None,
) -> go.Figure:
    """좌축: Mean Z-Score  |  우축: Std Z-Score  |  KOSPI 3번째 축. Plotly 인터랙티브."""
    fig = go.Figure()

    # 빨간 음영
    if shade_intervals:
        for start, end in shade_intervals:
            fig.add_vrect(x0=start, x1=end, fillcolor="red", opacity=0.15, layer="below", line_width=0)

    # 21일 EMA 적용
    mean_ema = mean_series.ewm(span=21, min_periods=1).mean()
    std_ema = std_series.ewm(span=21, min_periods=1).mean()

    # Mean Z-Score 21EMA (좌축 y)
    fig.add_trace(go.Scatter(
        x=mean_ema.index, y=mean_ema.values,
        name="Mean Z (21EMA)",
        line=dict(color="#2563eb", width=1.5),
        yaxis="y",
    ))

    # Std Z-Score 21EMA (우축 y2)
    fig.add_trace(go.Scatter(
        x=std_ema.index, y=std_ema.values,
        name="Std Z (21EMA)",
        line=dict(color="#10b981", width=1.3),
        opacity=0.9,
        yaxis="y2",
    ))

    # KOSPI 로그 누적수익률 (3번째 축 y3)
    kospi_aligned = kospi.reindex(mean_series.index, method="ffill").dropna()
    log_ret = np.log(kospi_aligned / kospi_aligned.shift(1)).fillna(0)
    cum_log_ret = log_ret.cumsum() * 100  # % 단위
    fig.add_trace(go.Scatter(
        x=cum_log_ret.index, y=cum_log_ret.values,
        name="KOSPI 누적로그수익률(%)",
        line=dict(color="#f59e0b", width=1.2),
        opacity=0.55,
        yaxis="y3",
    ))

    # y=0 기준선
    fig.add_hline(y=0, line_dash="dash", line_color="gray", line_width=0.7, yref="y")

    # Y축 밴드: 데이터 범위보다 약간만 여유를 두어 변동성이 잘 보이도록
    mean_pad = (mean_ema.max() - mean_ema.min()) * 0.05
    std_pad = (std_ema.max() - std_ema.min()) * 0.05

    fig.update_layout(
        title=dict(text=title, font=dict(size=13, family="sans-serif")),
        height=450,
        margin=dict(r=110, t=50, b=50),
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(x=0.01, y=0.99, font=dict(size=10), bgcolor="rgba(255,255,255,0.7)"),
        xaxis=dict(showgrid=False, tickformat="%Y-%m"),
        yaxis=dict(
            title=dict(text="Mean Z (21EMA)", font=dict(color="#2563eb")),
            tickfont=dict(color="#2563eb"),
            showgrid=False,
            range=[mean_ema.min() - mean_pad, mean_ema.max() + mean_pad],
        ),
        yaxis2=dict(
            title=dict(text="Std Z (21EMA)", font=dict(color="#10b981")),
            tickfont=dict(color="#10b981"),
            overlaying="y",
            side="right",
            showgrid=False,
            range=[std_ema.min() - std_pad, std_ema.max() + std_pad],
        ),
        yaxis3=dict(
            title=dict(text="KOSPI 누적로그수익률(%)", font=dict(color="#b45309")),
            tickfont=dict(color="#b45309"),
            overlaying="y",
            side="right",
            position=0.93,
            showgrid=False,
        ),
    )

    return fig


def main() -> None:
    st.title("산업별 Z-Score 대시보드")
    st.caption("외부 수집 없이 현재 저장소 CSV만 사용")

    pbr_df, price_df = load_local_data()
    kospi = load_kospi_data()
    heatmap_data = build_heatmap_data(pbr_df, price_df)
    ts_data = build_timeseries_data(pbr_df, price_df)

    latest = heatmap_data["pbr_1Y"].index.max()
    st.write(f"기준 월말 거래일: {latest:%Y-%m-%d}")
    st.markdown("---")

    # ── 1Y 히트맵 ──────────────────────────────────────────────────────────
    st.subheader("1Y Rolling Z-Score 히트맵")
    left, right = st.columns(2)
    with left:
        st.pyplot(render_heatmap(heatmap_data["pbr_1Y"], "PBR Z-Score (1Y)"))
    with right:
        st.pyplot(render_heatmap(heatmap_data["mom_1Y"], "MOM Z-Score (1Y)"))

    st.markdown("---")

    # ── 일별 Cross-Sectional Z-Score (Mean + Std + KOSPI 통합 차트) ────────
    st.subheader("일별 Cross-Sectional Z-Score — Mean & Std + KOSPI")
    st.caption("좌축: Mean / Std Z-Score  |  우축: KOSPI  |  빨간 음영: Std Z-Score > 1.65 이후 21 영업일")

    pbr_shade = compute_shade_intervals(ts_data["pbr_std_zscore"])
    mom_shade = compute_shade_intervals(ts_data["mom_std_zscore"])

    col_mom, col_pbr = st.columns(2)
    with col_mom:
        st.plotly_chart(render_combined_zscore_chart(
            mean_series=ts_data["mom_mean"],
            std_series=ts_data["mom_std"],
            kospi=kospi,
            title="MOM Z-Score — 섹터 평균 / 표준편차 + KOSPI",
            shade_intervals=mom_shade,
        ), use_container_width=True)
    with col_pbr:
        st.plotly_chart(render_combined_zscore_chart(
            mean_series=ts_data["pbr_mean"],
            std_series=ts_data["pbr_std"],
            kospi=kospi,
            title="PBR Z-Score — 섹터 평균 / 표준편차 + KOSPI",
            shade_intervals=pbr_shade,
        ), use_container_width=True)


if __name__ == "__main__":
    main()
