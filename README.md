# Trade Analytics

將 UN Comtrade 月度進口資料擷取、驗證並載入 BigQuery，再透過 dbt 建立進口趨勢、貿易夥伴占比、商品排名與集中度分析。Airflow 提供手動觸發的串接流程。

## 現況與限制

- 來源為 Preview API，品質標記為 `BOUNDED_PREVIEW`；通過驗證不代表取得完整市場資料。
- 支援六位數 HS 商品代碼、`YYYYMM` 月份、正整數 reporter，以及 `world_total`／`partner_detail`。
- Raw 僅支援首次載入；相同範圍已存在時會拒絕，不提供歷史修訂覆寫或自動補數。
- DAG 為手動觸發、`catchup=False`、單一執行中批次，ingestion 不自動重試。
- World Total 與 Partner Detail 不可相加，缺少紀錄也不等於零。

## 文件導覽

| 文件 | 用途 |
|---|---|
| [環境設定](docs/setup.md) | 三個 Python 環境、依賴、認證、變數與 dbt profile |
| [操作手冊](docs/runbook.md) | 初始化、試跑、寫入、dbt 與 Airflow |
| [復原手冊](docs/recovery.md) | 失敗分類、原 Job 查核、證據保留與復原 |
| [測試指南](docs/testing.md) | 離線、整合、dbt 測試與驗收腳本 |
| [架構](docs/architecture.md) | 完整資料流及模組責任 |
| [資料契約](docs/data-contracts.md) | 欄位、粒度、識別碼、精度與品質限制 |
| [分析範例](docs/analytics-examples.md) | 四個 Mart 的查詢與解讀 |
| [dbt 模型總覽](dbt/docs/overview.md) | 模型分層及指標定義 |

## 開始使用

先完成環境設定；以下由專案根目錄執行，只檢查本機路徑，不測試雲端連線：

```bash
export GCP_PROJECT_ID="your-project-id"
export PYTHONPATH="$PWD/src"
.venv/bin/python -m trade_analytics.orchestration.runner check
```

離線 ETL 測試：

```bash
.venv/bin/python -m pytest -m "not integration" --ignore=tests/test_airflow_dag.py
```

正式資料路徑使用 Runner 的 `ingest`。`trade_analytics.main` 是批次擷取與 manifest 驗證入口，不會執行完整的 Landing、Raw 與 dbt 載入。

## 目錄

`src/`：擷取、契約、轉換、倉儲與 Runner；`dags/`：Airflow；`dbt/`：模型與 SQL 測試；`scripts/`：倉儲檢查和驗收；`tests/`：Python 測試；`docs/`：操作與設計文件。

變更資料契約、CLI 或環境依賴時，應同步更新對應文件並執行相關測試。設定範例不包含憑證；本機認證與執行產物不應提交。
