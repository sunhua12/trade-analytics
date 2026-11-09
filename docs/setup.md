# 環境設定

[回到首頁](../README.md)

## Python 與依賴

本次文件基準為本機 Python `3.14.6`。Runner 固定使用 `.venv/bin/python` 與 `.venv-dbt/bin/dbt`；DAG 固定使用 `.venv-airflow/bin/python`。請保留這三個目錄名稱。

```bash
python3.14 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
python3.14 -m venv .venv-dbt
.venv-dbt/bin/python -m pip install -r requirements-dbt.txt
python3.14 -m venv .venv-airflow
.venv-airflow/bin/python -m pip install -r requirements-airflow.txt

.venv/bin/python -m pip check
.venv-dbt/bin/python -m pip check
.venv-airflow/bin/python -m pip check
```

ETL 的正式執行只需 `requirements.txt`，開發測試使用 `requirements-dev.txt`。Airflow 透過子程序呼叫 ETL 和 dbt，不需將三份依賴裝在同一環境。

版本取自現有本機安裝：Airflow `3.4.0`、dbt-core `1.13.0rc1`、dbt-bigquery `1.12.2`。保留現有 dbt 預發行版本，不在文件補齊時順便升降版。清單鎖定直接依賴，**不是完整的跨平台遞移依賴 lock**；全新環境仍須完成 `pip check` 與測試，不能將本機測試通過當作乾淨安裝已驗證。

## 環境變數

從專案根目錄設定；程式不會自動載入 `.env`：

```bash
export TRADE_ANALYTICS_ROOT="$PWD"
export PYTHONPATH="$PWD/src"
export GCP_PROJECT_ID="your-project-id"
export BQ_LOCATION="asia-northeast1"
export BQ_RAW_DATASET="trade_raw"
export BQ_ANALYTICS_DATASET="trade_analytics_dev"
export DBT_PROFILES_DIR="$PWD/dbt"
```

| 變數 | 行為 |
|---|---|
| `GCP_PROJECT_ID` | 必填；範例值須替換 |
| `BQ_LOCATION` | 通用倉儲設定必填；Runner／smoke 預設 `asia-northeast1` |
| `BQ_RAW_DATASET` | Python 預設 `trade_raw`；目前 dbt source 寫死 `trade_raw`，完整流程請維持此值 |
| `BQ_ANALYTICS_DATASET` | Python 預設 `trade_analytics`；此手冊設為 `trade_analytics_dev`，讓檢查腳本與 dbt 開發目標一致 |
| `TRADE_ANALYTICS_ROOT` | Runner 可省略並由程式位置推導；設定時應為絕對路徑 |
| `PYTHONPATH` | 目前未設定可安裝的 Python package，CLI 由 `src` 載入 |
| `DBT_PROFILES_DIR` | 本例存放本機 `profiles.yml` 的目錄 |
| `RUN_COMTRADE_INTEGRATION` | 僅真實來源測試時設為 `1` |
| `RUN_BIGQUERY_INTEGRATION` | 僅執行雲端整合測試時設為 `1` |

`BQ_ANALYTICS_DATASET` 不會替 dbt 選擇 dataset；dbt 使用 profile 的 `dataset`。`verify_warehouse_setup` 明確要求 `dev` target 與 `trade_analytics_dev`。

## Google Cloud 認證與資料集

Python BigQuery client 使用 ADC。已有可用 ADC 的環境可沿用；本機互動登入可執行：

```bash
gcloud auth application-default login
```

執行身分需能在專案提交 BigQuery jobs、讀取來源資料與 dataset metadata。初始化需建立資料表；ingestion 需寫入 Landing、Raw、稽核表；dbt 需在開發資料集建立或取代模型。以實際操作所需權限配置身分，不將個人憑證放入 repo。

先在同一 location 建立 `trade_raw` 與 `trade_analytics_dev`；`provision_bigquery.py` 只建立資料表，**不建立 dataset**。例如使用具備建立權限的身分執行以下雲端變更：

```bash
bq --location="$BQ_LOCATION" mk --dataset "$GCP_PROJECT_ID:trade_raw"
bq --location="$BQ_LOCATION" mk --dataset "$GCP_PROJECT_ID:trade_analytics_dev"
```

既有 dataset 不需重建；應確認 location 相同。

## dbt profile

```bash
cp dbt/profiles.example.yml dbt/profiles.yml
.venv-dbt/bin/dbt debug --project-dir dbt --target dev
.venv/bin/python -m trade_analytics.orchestration.runner check
```

`dbt debug` 會驗證雲端連線；Runner `check` 只檢查本機執行檔及目錄，不代表 ADC、資料表或資料品質已通過。

接著依 [操作手冊](runbook.md) 初始化資料表。Airflow 環境設定也在該手冊。
