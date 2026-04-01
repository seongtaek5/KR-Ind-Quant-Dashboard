# KR-Ind-Quant-Dashboard

수동 실행 GitHub Actions로 아래 3개 데이터를 갱신하는 저장소입니다.

- 산업 ETF 일별 종가: `kodex_sector_etf_close.csv`
- 코스피 일별 종가: `kospi_daily.csv`
- 산업별 PBR 일별 데이터: `krx_sector_pbr.csv`

대시보드는 로컬 CSV를 읽어 TS-PBR Z-SCORE, TS-MOM Z-SCORE를 시각화합니다.

## 구성 파일

- `price_data_collect.py`: pykrx로 산업 ETF/코스피 데이터 수집
- `krx_pbr_pipeline.py`: KRX 쿠키 기반 산업별 PBR 수집
- `sector_zscore_app.py`: Streamlit 시각화 앱
- `.github/workflows/update-market-data.yml`: 수동 실행 액션

## GitHub Actions 실행 방법

1. GitHub 저장소의 Actions 탭에서 `Update KRX Market Data` 워크플로우를 선택합니다.
2. `Run workflow`를 누른 뒤 입력값을 넣습니다.
3. `krx_cookie`에는 KRX 로그인 후 Network의 `getJsonData.cmd` 요청 헤더의 Cookie 전체를 붙여넣습니다.
4. `yyyymmdd`는 선택 입력입니다. 비우면 실행 당일 기준으로 수집합니다.

정상 완료 시 변경된 CSV만 자동 커밋/푸시됩니다.

## 로컬 실행

```bash
pip install -r requirements.txt

# 가격 데이터 (ETF/코스피)
python price_data_collect.py

# PBR 데이터 (쿠키 필요)
export KRX_COOKIE='여기에 KRX Cookie 전체'
python krx_pbr_pipeline.py

# 대시보드
streamlit run sector_zscore_app.py
```

## 로컬 수동 실행 후 main 자동 반영

VS Code 터미널에서 아래처럼 실행하면 데이터 수집부터 CSV 3개 커밋/푸시까지 한 번에 처리됩니다.

```bash
export KRX_COOKIE='여기에 KRX Cookie 전체'
bash run_local_update_and_push.sh
```

동작 순서:

1. `origin/main` 최신 반영 (`git pull --rebase origin main`)
2. `price_data_collect.py` 실행
3. `krx_pbr_pipeline.py` 실행
4. CSV 3개만 커밋
5. `main` 브랜치로 푸시

원하면 VS Code에서 `Run Task` -> `Update CSV and Push Main`으로 실행할 수 있습니다.