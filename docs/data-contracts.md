# 資料契約

[回到首頁](../README.md)

## Query 與粒度

`ComtradeQuery` 接受合法 `YYYYMM`、正整數 `reporter_code`、六位數字串 `cmd_code` 及兩種 `query_type`。固定進口 `flowCode=M`、月頻 `freqCode=M`、`partner2Code=0`、`customsCode=C00`、`motCode=0`，Raw 驗證要求原始分類旗標為 true。

World Total 的 `partnerCode=0`；Partner Detail 排除 `partnerCode=0`。兩者是不同 query scope。候選紀錄粒度包含 period、reporter、partner、商品、flow、classification、frequency、partner2、customs、transport mode 與原始分類旗標；Business Key 還包含 source query key。重複時拒絕，不默默去重。

## 欄位對照

| API 欄位 | Landing 欄位 | Raw 欄位／行為 |
|---|---|---|
| `period` | `period` | 保留並產生月份首日 `period_start_date` |
| `reporterCode`／`partnerCode` | `reporter_code`／`partner_code` | INT64 |
| `cmdCode` | `cmd_code` | STRING，保留前導零 |
| `primaryValue` | `primary_value_text` | `primary_value`，BIGNUMERIC，必填 |
| `netWgt` | `net_weight_text` | `net_weight`，BIGNUMERIC，可空 |
| `qty` | `quantity_text` | `quantity`，BIGNUMERIC，可空 |
| `flowCode`／`classificationCode`／`freqCode` | 對應 snake_case 欄位 | 保留來源維度 |
| `partner2Code`／`customsCode`／`motCode` | 對應 snake_case 欄位 | 必須符合支援範圍 |
| `isOriginalClassification` | `is_original_classification` | BOOL |
| 完整單筆來源 | `source_record_json` | 保留以供 checksum 與追查 |

`hs_version` 目前轉換為 NULL，不推測來源未提供的版本。若非空，Raw contract 要求其與 `classification_code` 相符。其餘核准展示、參考與品質欄位保留在來源 JSON，不等於已建立可分析的獨立欄位。

數值使用 int／Decimal，拒絕 float、bool、非有限值、超過 BIGNUMERIC 範圍及有效小數位超過 38 位的值，不以四捨五入繞過驗證。Decimal 在來源 JSON 中以字串保存。

## 身分與證據

| 識別／證據 | 定義 |
|---|---|
| `batch_id` | 每次 smoke 呼叫建立新的 UUID；重跑不沿用 |
| `source_query_key` | 帶 key version 的 canonical query scope SHA-256 |
| `source_row_index` | 過濾後來源順序，從 0 起連續編號 |
| 來源 SHA-256 | 來源紀錄依既定序列化與順序計算，驗證內容與順序 |
| NDJSON SHA-256 | Landing 傳輸內容的 checksum，不能與來源 checksum 混用 |
| `revision_id` | `SHA256(source_query_key + ':' + source_sha256)` |
| Raw job ID | `raw_` 加上 batch、query key、revision 組合的 SHA-256 |

Raw 依月份分區，依 reporter／商品叢集。載入後核對 scope 筆數、batch、query key、revision、連續 row index 及來源內容 checksum。

## 完整性與稽核

Preview 回應上限為 500，回應 count 必須等於實際長度且小於門檻；達門檻視為可能截斷。未核准的來源欄位、粒度變化、來源參考欄位不一致與空 Raw 批次均應停止，不能靠移除欄位或補零通過。

`BOUNDED_PREVIEW` 只表示在目前回應與契約範圍內通過驗證。數值對帳成功仍不代表完整來源、缺少夥伴不等於零，商品排名也不是全市場排名。

`ingestion_runs` 保存事件，狀態包含 `STARTED`、`SUCCEEDED`、`FAILED`、`VALIDATION_FAILED` 與 `UNKNOWN`。Landing 與 Raw 共用此表；以 job ID 與 revision 區分階段，不能只看同 batch 最新一筆。復原成功會追加成功事件，不保證一個 job 只有一筆成功紀錄。

欄位完整型別以 `warehouse/bigquery_schema.py` 為準，身份以 `contracts/query_identity.py` 為準。修改時應同步對照 [架構](architecture.md)、測試與 dbt schema。
