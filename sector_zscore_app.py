"""산업별 Time-Series Z-Score 히트맵 Streamlit 앱 (로컬 CSV 기반)."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import pandas as pd
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
def build_heatmap_data(
    _pbr_df: pd.DataFrame,
    _price_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    pbr_daily, price_daily = prepare_aligned_data(_pbr_df, _price_df)

    pbr_z = rolling_zscore(pbr_daily, windows=[252, 504, 1260])
    mom_z = rolling_zscore(price_daily.pct_change() * 100, windows=[252, 504, 1260])

    # 한국 벤치마크: 섹터 일별 수익률의 단순평균
    benchmark_ret = price_daily.pct_change().mean(axis=1, skipna=True) * 100
    benchmark_z = rolling_zscore(benchmark_ret.to_frame(BENCHMARK_LABEL), windows=[252, 504, 1260])

    max_date = pbr_daily.index.max()
    one_year_ago = max_date - pd.DateOffset(years=1)
    subset = pbr_daily.loc[pbr_daily.index >= one_year_ago]
    try:
        month_end_idx = (
            subset.groupby(subset.index.to_period("M"))
            .apply(lambda x: x.index.max(), include_groups=False)
        )
    except TypeError:
        # pandas < 2.2 does not have include_groups parameter
        month_end_idx = (
            subset.groupby(subset.index.to_period("M"))
            .apply(lambda x: x.index.max())
        )

    heatmap_data: dict[str, pd.DataFrame] = {}

    for window in ["1Y", "2Y", "5Y"]:
        pbr_month = pbr_z[window].loc[month_end_idx].copy()
        mom_month = mom_z[window].loc[month_end_idx].copy()
        bench_month = benchmark_z[window].loc[month_end_idx].copy()

        pbr_month[BENCHMARK_LABEL] = bench_month[BENCHMARK_LABEL].to_numpy()
        mom_month[BENCHMARK_LABEL] = bench_month[BENCHMARK_LABEL].to_numpy()

        heatmap_data[f"pbr_{window}"] = pbr_month
        heatmap_data[f"mom_{window}"] = mom_month

    return heatmap_data


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


def main() -> None:
    st.title("산업별 Time-Series PBR/MOM Z-Score 히트맵")
    st.caption("외부 수집 없이 현재 저장소 CSV만 사용")

    pbr_df, price_df = load_local_data()
    heatmap_data = build_heatmap_data(pbr_df, price_df)

    latest = heatmap_data["pbr_1Y"].index.max()
    st.write(f"기준 월말 거래일: {latest:%Y-%m-%d}")
    st.markdown("---")

    for window in ["1Y", "2Y", "5Y"]:
        st.subheader(f"{window} Rolling")
        left, right = st.columns(2)

        with left:
            st.pyplot(render_heatmap(heatmap_data[f"pbr_{window}"], f"PBR Z-Score ({window})"))

        with right:
            st.pyplot(render_heatmap(heatmap_data[f"mom_{window}"], f"MOM Z-Score ({window})"))

        if window != "5Y":
            st.markdown("---")


if __name__ == "__main__":
    main()
