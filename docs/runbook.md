# 操作手冊

[回到首頁](../README.md) · 前置：[環境設定](setup.md)

以下由根目錄執行，已設定 ADC、`PYTHONPATH`、GCP 與 dbt 變數。範例查詢使用 reporter `842`、月份 `202401`、商品 `854231`，請依需求更換；商品必須為六位數 HS 代碼。

## 初始化與確認

```bash
.venv/bin/python scripts/check_bigquery.py
.venv/bin/python scripts/provision_bigquery.py
```

前者核對 dataset 與 location；後者讀取現有資料表並列出缺少的表，尚未建立資料表。確認目標後執行實際建立：

```bash
.venv/bin/python scripts/provision_bigquery.py --apply
.venv-dbt/bin/dbt run-operation verify_warehouse_setup --project-dir dbt --target dev
```

初始化包含 `trade_raw.comtrade_landing`、`comtrade_records`、`ingestion_runs`。既有 schema 不符時應調查差異，不刪表重建以繞過檢查。

## 來源試跑

```bash
.venv/bin/python -m trade_analytics.orchestration.runner ingest   --period 202401 --reporter 842 --cmd 854231 --query-type world_total
```

此步驟會呼叫真實 UN Comtrade API、驗證來源並查詢 BigQuery 中的 Raw scope；可能產生查詢費用，並非離線模擬。不會寫入 Landing、Raw 或稽核表。需要既有 Raw 表與有效認證；相同 scope 已存在也會拒絕。

預期日誌為 `DRY RUN 成功`，包含批次識別、筆數與來源 checksum。

## 正式首次載入

```bash
.venv/bin/python -m trade_analytics.orchestration.runner ingest   --period 202401 --reporter 842 --cmd 854231 --query-type world_total   --apply --allow-bounded-preview
```

兩個旗標同時提供才允許寫入；第二個旗標表示接受來源完整性限制。載入 Partner Detail 時，另執行一次並把 `--query-type` 改為 `partner_detail`。兩種 query type 是不同 scope，不能視為同一批資料相加。

流程依序驗證來源、寫入 Landing、驗證 Landing 筆數、寫入 Raw、讀回驗證與紀錄稽核。成功應見 `PRODUCTION SMOKE SUCCESS` 與 Raw job ID；單看程序結束或一筆 `STARTED` 不足以判定成功。

保存本次參數、完整日誌、`batch_id`、`source_query_key`、來源 SHA-256、revision 與 job ID。現有 CLI **不會自動保存完整復原 bundle**；需要保證可復原的呼叫端應依 [復原手冊](recovery.md) 在寫入前保存原始 payload 與 report。

重跑同一個 CLI 會產生新的 `smoke-UUID`。若出錯，先進入復原流程，不將重新觸發當作重試原 Job。

## dbt 建構與驗收

```bash
.venv/bin/python -m trade_analytics.orchestration.runner dbt-build
```

固定使用 `dev` target，但實際資料集仍由 profile 決定；本專案使用 `trade_analytics_dev`。此指令會執行 BigQuery jobs 並建立或取代模型。

完整驗收與 docs 產生：

```bash
PATH="$PWD/.venv-dbt/bin:$PATH" bash scripts/validate_dbt.sh
```

失敗時腳本立即停止；成功訊息只會在所有步驟完成後輸出。詳見 [測試指南](testing.md)。

## Airflow 本機手動流程

以下為本機開發用 standalone，非正式服務部署規格：

```bash
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
.venv-airflow/bin/airflow standalone
```

從同一個已設定 GCP／dbt／`PYTHONPATH` 的 shell 啟動，讓任務繼承設定。依終端輸出的網址與登入方式進入 UI，確認 `airflow_environment_check` 與 `trade_analytics_pipeline` 可見，無匯入錯誤。

啟用 `trade_analytics_pipeline` 後手動 Trigger，參數如下：

```json
{"period":"202401","reporter":842,"cmd":"854231","query_type":"world_total","apply":true,"allow_bounded_preview":true}
```

這是實際寫入，不能拿來做 dry run。DAG 順序為 `run_ingestion → dbt_build`，`schedule=None`、`catchup=False`、`max_active_runs=1`、`retries=0`。單一 DAG 的並行限制不會阻止其他 CLI 或外部程序同時寫入。

ingestion 失敗時不得直接清除並重跑該 task；先對照原 Job 與稽核紀錄。若 ingestion 已確認成功、只有 dbt 失敗，可修正後單獨執行 Runner `dbt-build`，避免重新載入來源。停止本機 standalone 使用原終端的 Ctrl-C；保留 `.airflow` 中的執行紀錄以便追查。
