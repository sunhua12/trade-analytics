# 測試與驗收

[回到首頁](../README.md) · 前置：[環境設定](setup.md)

## 離線 Python 測試

從根目錄執行；ETL 與 Airflow 分環境驗證，避免收集 DAG 測試時缺少 Airflow：

```bash
.venv/bin/python -m pytest -m "not integration" --ignore=tests/test_airflow_dag.py
(
  export AIRFLOW_HOME="$(mktemp -d "${TMPDIR:-/tmp}/trade-analytics-airflow.XXXXXX")"
  export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
  export AIRFLOW__CORE__LOAD_EXAMPLES=False
  .venv-airflow/bin/airflow db migrate &&
    .venv-airflow/bin/python -m pytest tests/test_airflow_dag.py
)
```

第一組涵蓋來源回應、欄位型別、Query identity、精度、NDJSON、重試、BigQuery mock、載入決策、復原及 Runner；第二組確認 DAG 發現、順序、參數、失敗傳遞與停用自動重試。DAG 測試中的 subprocess 已 mock，不會執行真正 ingestion 或 dbt build。`DagBag.get_dag()` 仍需要 metadata database，因此指令在獨立暫存目錄初始化 SQLite，不碰既有 Airflow 資料庫；子 shell 結束不改變原環境變數，暫存目錄保留供追查。

只測這次相關行為時，可執行 `tests/test_raw_load_coordinator.py`、`tests/test_orchestration_runner.py` 與 `tests/test_production_smoke.py`。

## 真實來源測試

`tests/test_extract.py::test_api` 已標記為 integration，另需明確旗標才呼叫真實 API：

```bash
RUN_COMTRADE_INTEGRATION=1 .venv/bin/python -m pytest tests/test_extract.py::test_api
```

此測試需要網路，不寫入 BigQuery。不要將來源當下可用性混入離線驗收。

## BigQuery 整合測試

以下會建立、寫入、查詢及清除暫存表，並可能產生費用；使用開發／測試專案，先完成 ADC 與 dataset 設定：

```bash
RUN_BIGQUERY_INTEGRATION=1 .venv/bin/python -m pytest -m integration   tests/test_bigquery_ingestion.py tests/test_bigquery_raw_initial.py
```

未設定旗標時測試會跳過，跳過不代表整合通過。`test_bigquery_raw_initial.py` 使用預設 `trade_raw`，不能只改 `BQ_RAW_DATASET` 就假設已完全隔離；以獨立 `GCP_PROJECT_ID` 區隔環境。測試建立唯一名稱與到期時間，正常結束會清理；程序中斷時需檢查本次暫存表。

## dbt 驗收

```bash
PATH="$PWD/.venv-dbt/bin:$PATH" bash scripts/validate_dbt.sh
```

腳本依序執行版本資訊、`dbt debug`、`dbt parse`、受控 unit tests、完整 `dbt build`、data tests 與 `dbt docs generate`。全部使用 `dev` target，任一步驟非零退出即停止。

即使使用固定 fixture 的 dbt unit tests，也可能透過 BigQuery 執行 SQL，**不屬於離線測試**。完整 build 會建立或取代實體模型。要個別執行時：

```bash
.venv-dbt/bin/dbt test --project-dir dbt --target dev --select test_type:unit
.venv-dbt/bin/dbt build --project-dir dbt --target dev
.venv-dbt/bin/dbt test --project-dir dbt --target dev --select test_type:data
.venv-dbt/bin/dbt docs generate --project-dir dbt --target dev
```

unit tests 檢查已知答案與缺月份、零基期等情境；data tests 檢查現有倉儲粒度、關聯、指標及來源一致性。YoY 案例應放在 `dbt/models/marts/_marts_unit_tests.yml`，不能把 Markdown 或 YAML 範例貼入 Shell 腳本。

## 本機靜態檢查與驗收紀錄

```bash
bash -n scripts/validate_dbt.sh
git diff --check
.venv/bin/python -m pip check
.venv-dbt/bin/python -m pip check
.venv-airflow/bin/python -m pip check
```

驗收紀錄需分別列出環境版本、執行命令、通過／失敗／跳過數，以及是否有執行真實 API 或 BigQuery。離線通過不代表雲端 IAM、location 或來源當日可用性已驗證。

dbt 產物位於 `dbt/target`、日誌位於 `dbt/logs`；檢查失敗節點與原始錯誤後再決定重跑範圍。程式只有在全部命令成功後才顯示腳本成功訊息。
