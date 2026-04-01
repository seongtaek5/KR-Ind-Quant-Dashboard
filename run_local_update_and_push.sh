#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_ROOT"

PYTHON_BIN="python"
if [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then
  PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
fi

# 데이터 조회 실패를 조용히 넘기지 않고 즉시 실패 처리
export KRX_STRICT="${KRX_STRICT:-1}"

CSV_FILES=(
  "kodex_sector_etf_close.csv"
  "kospi_daily.csv"
  "krx_sector_pbr.csv"
)

echo "[1/5] 최신 main 동기화"
git fetch origin main
# 로컬 main에 올라온 최신 변경을 먼저 반영해 non-fast-forward를 방지
if [[ "$(git rev-parse --abbrev-ref HEAD)" != "main" ]]; then
  echo "현재 브랜치는 main이 아닙니다. main으로 전환 후 다시 실행하세요."
  exit 1
fi
git pull --rebase origin main

echo "[2/5] ETF/KOSPI 수집"
"$PYTHON_BIN" price_data_collect.py

echo "[3/5] 산업 PBR 수집"
if [[ -z "${KRX_COOKIE:-}" ]]; then
  echo "KRX_COOKIE 환경변수가 비어 있습니다."
  echo "예: export KRX_COOKIE='여기에 쿠키 전체'"
  exit 1
fi
"$PYTHON_BIN" krx_pbr_pipeline.py

echo "[4/5] CSV 변경 커밋"
git add "${CSV_FILES[@]}"
if git diff --cached --quiet; then
  echo "CSV 변경 사항이 없어 종료합니다."
  exit 0
fi

run_date="${KRX_TODAY:-$(date +%Y%m%d)}"
git commit -m "chore(data): local update ${run_date}"

echo "[5/5] main 푸시"
git push origin main

echo "완료: CSV 업데이트가 main에 반영되었습니다."
