# 失敗查核與 Raw 復原

[回到首頁](../README.md) · [操作手冊](runbook.md)

## 先確認原批次

停止同 scope 的新載入，保留完整日誌與原始查詢參數。不要重新呼叫 API 來取代原來源，也不要直接清除 Airflow ingestion task；新執行會產生不同 batch ID。

由日誌取得 `batch_id`、`source_query_key`、來源 SHA-256、Raw job ID 與 revision。以 BigQuery Standard SQL 查詢稽核事件，將專案與 batch 換成原值：

```sql
SELECT batch_id, source_query_key, revision_id, load_job_id,
       status, record_count, loaded_record_count, sha256,
       started_at, completed_at, error_message
FROM `YOUR_PROJECT.trade_raw.ingestion_runs`
WHERE batch_id = 'ORIGINAL_BATCH_ID'
ORDER BY started_at, completed_at;
```

Landing 和 Raw 共用稽核表，不能只用最後一筆代表全部成功。Raw job ID 以 `raw_` 開頭，Raw audit 的 SHA-256 為來源內容 checksum；Landing audit 的 checksum 代表 NDJSON。

從 BigQuery Job 詳情核對 project、location、job ID、狀態與錯誤；CLI 唯讀查看範例：

```bash
bq --project_id="$GCP_PROJECT_ID" --location="$BQ_LOCATION" \
  show --job=true --format=prettyjson ORIGINAL_RAW_JOB_ID
```

查不到 Job 不等於 Job 未執行；可能是權限、region、保存期限或連線問題。

## 判斷與處置

| 情況 | 處置 |
|---|---|
| 來源驗證失敗、尚未寫入 | 查核 envelope、粒度、未知欄位與門檻；修正原因後才開始新批次 |
| Landing 失敗或狀態不明 | 查核 Landing load job 與實際資料；此手冊的 Raw recovery 不處理 Landing 重送 |
| Raw Job 尚未 DONE／無法查明 | 保留 `UNKNOWN`，稍後查詢原 Job，不重新提交 |
| Raw Job DONE 且有 error | 已確認失敗；先分析錯誤與既有 Raw，再決定新載入，不使用成功復原函式 |
| Raw Job DONE、無 error，但成功 audit 缺少 | 使用下方原始證據驗證並追加成功 audit |
| `VALIDATION_FAILED`／checksum 或筆數不符 | 停止後續 dbt，保留證據；不可刪資料、補零或換 checksum 讓它通過 |
| `RawScopeAlreadyExists` | 查核既有資料的 batch／revision；可能原批次已完成，不表示要覆寫 |
| ingestion 已驗證成功，只有 dbt 失敗 | 修正 dbt 問題後單獨執行 Runner `dbt-build` |

## 復原必要證據與保存方式

`recover_initial_raw()` 需要原始 `LandingPayload`、`ComtradeQuery`、`SourceQualityReport`，不能只給 job ID。現有 CLI 不會自動將這三者輸出成檔案；若只有日誌與 audit，不得捏造 report 的 response count 或以新 API 回應替代原證據。

以下是供直接使用 Python ingestion API 的呼叫端，在來源驗證及 payload 建立後、**任何 Landing／Raw 寫入前**保存的 bundle 格式。變數 `payload`、`comtrade_query` 與 `report` 必須是該次真正驗證通過的物件：

```python
import json
from dataclasses import asdict
from pathlib import Path

artifact = {
    "payload": asdict(payload),
    "query": comtrade_query.model_dump(mode="json"),
    "report": asdict(report),
}
folder = Path("recovery-artifacts")
folder.mkdir(exist_ok=True)
path = folder / f"{payload.batch_id}.json"
with path.open("x", encoding="utf-8") as stream:
    json.dump(artifact, stream, ensure_ascii=False, indent=2, allow_nan=False)
```

`LandingPayload` 內來源 JSON 與數值已經是字串；保持其內容與 row order，不重新格式化內層 JSON。復原資料夾已加入 `.gitignore`；需另外備份原檔，不能把忽略追蹤視為持久保存。

對沒有 bundle 的舊批次，可以唯讀匯出原 Landing rows 並按 `source_row_index` 排序來查核 payload；但 report 仍需原始保存證據或可核驗的原回應。若無法取得，停止在查核階段，記錄「缺少原始復原證據」，不宣稱已復原。自動保存 bundle 與專用 recovery CLI 尚未實作。

## 使用既有 API 復原

先設定 [環境變數](setup.md)，設定以下原值：

```bash
export RECOVERY_BUNDLE="recovery-artifacts/ORIGINAL_BATCH_ID.json"
export EXPECTED_RAW_JOB_ID="raw_ORIGINAL_JOB_HASH"
export EXPECTED_REVISION_ID="ORIGINAL_REVISION_HASH"
```

以下會查詢原 Job、驗證 Raw 筆數／身分／row index／checksum，成功後**追加 `SUCCEEDED` audit**，不提交新的 Raw INSERT。需有 audit 寫入權限，不屬於純唯讀操作：

```bash
.venv/bin/python - <<'PY'
import json
import os
from pathlib import Path
from google.cloud import bigquery
from trade_analytics.ingestion.query import ComtradeQuery
from trade_analytics.processing.landing_payload import LandingPayload
from trade_analytics.contracts.source_completeness import SourceQualityReport
from trade_analytics.warehouse.bigquery_config import load_bigquery_config
from trade_analytics.warehouse.raw_load_coordinator import (
    validate_raw_source, build_raw_identity, recover_initial_raw,
)

artifact = json.loads(Path(os.environ["RECOVERY_BUNDLE"]).read_text())
payload = LandingPayload(**artifact["payload"])
query = ComtradeQuery.model_validate(artifact["query"])
report_values = dict(artifact["report"])
report_values["unmodeled_fields"] = tuple(report_values["unmodeled_fields"])
report = SourceQualityReport(**report_values)
scope = validate_raw_source(payload, query, report)
revision_id, job_id = build_raw_identity(scope, report)
if job_id != os.environ["EXPECTED_RAW_JOB_ID"]:
    raise ValueError("Bundle 與原始 Job ID 不一致，停止")
if revision_id != os.environ["EXPECTED_REVISION_ID"]:
    raise ValueError("Bundle 與原始 Revision 不一致，停止")
config = load_bigquery_config()
with bigquery.Client(project=config.project_id, location=config.location) as client:
    result = recover_initial_raw(client, config, payload, query, report)
print(result)
PY
```

不可使用 `run_initial_raw_with_audit()` 代替此復原函式。若原 Job 未完成、確定失敗或 Raw 驗證失敗，函式拋出例外，不應繼續執行 dbt。

## 復原後驗收

確認回傳的 batch、revision 與 job ID 均為原值，record count 與原 payload 相同；重新查閱 audit，應見成功事件及正確 loaded count。復原重複執行可能追加多筆成功事件，所以不要用成功事件數估計載入次數。

保留原始錯誤、查核結果、復原命令與時間。只有 Raw 驗證通過後，才依操作手冊單獨執行 dbt build；若 audit 追加失敗，仍需保留錯誤並處理紀錄問題，不能據此認定 Raw INSERT 失敗。
