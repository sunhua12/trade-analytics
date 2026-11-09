# 分析查詢與解讀

[回到首頁](../README.md) · [dbt 模型總覽](../dbt/docs/overview.md)

以下為 BigQuery Standard SQL，將 `YOUR_PROJECT` 替換成實際專案。查詢會使用雲端資源；需先完成 dbt build。範例以觀察到的資料為範圍，空結果不代表進口值為零。

## 月度進口趨勢

```sql
SELECT period, reporter_code, cmd_code, classification_code, hs_version,
       import_value, mom_status, yoy_status,
       100 * mom_growth_rate AS mom_percent,
       100 * yoy_growth_rate AS yoy_percent,
       source_assurance_level
FROM `YOUR_PROJECT.trade_analytics_dev.mart_import_trends`
WHERE reporter_code = 842 AND cmd_code = '854231'
  AND period_start_date BETWEEN DATE '2024-01-01' AND DATE '2025-12-01'
ORDER BY period, classification_code, hs_version;
```

成長率原值 `0.2` 表示 20%。只在 `COMPARABLE` 時計算；缺前月／去年同月、零基期或負值產生相應狀態及 NULL。不能將 NULL 改為零成長，也不能把不連續月份的上一筆當前月。

## 貿易夥伴占比

```sql
SELECT partner_code, classification_code, hs_version, partner_import_value,
       100 * share_of_world AS world_percent,
       100 * share_of_reported_partners AS observed_partner_percent,
       partner_rank, partner_coverage_ratio, share_quality_status,
       source_assurance_level
FROM `YOUR_PROJECT.trade_analytics_dev.mart_partner_shares`
WHERE reporter_code = 842 AND cmd_code = '854231'
  AND period_start_date = DATE '2024-01-01'
ORDER BY classification_code, hs_version, partner_rank;
```

`share_of_world` 分母是 World Total；`share_of_reported_partners` 分母是回傳夥伴合計。後者加總接近 100% 不能證明來源完整。`PROVISIONAL_COMPARISON` 仍是暫定比較；缺 World Total、零總額或夥伴合計大於世界總額時需查看品質狀態。

## 觀察商品排名

```sql
SELECT cmd_code, classification_code, hs_version, commodity_code_length,
       import_value, commodity_rank, observed_commodity_count, ranking_status
FROM `YOUR_PROJECT.trade_analytics_dev.mart_product_rankings`
WHERE reporter_code = 842
  AND period_start_date = DATE '2024-01-01'
ORDER BY classification_code, hs_version, commodity_code_length, commodity_rank;
```

排名只比較同 scope、分類版本與代碼長度的已觀察商品，並列採 dense rank。單一觀察商品的第一名不表示市場第一名；負值會標記無效並留下 NULL rank。不要跨不同分類版本合併排名。

## 已回傳夥伴集中度

```sql
SELECT cmd_code, classification_code, hs_version, observed_partner_count,
       hhi_reported_partners, effective_partner_count,
       reconciliation_status, concentration_quality_status,
       source_assurance_level
FROM `YOUR_PROJECT.trade_analytics_dev.mart_trade_concentration`
WHERE reporter_code = 842 AND cmd_code = '854231'
  AND period_start_date = DATE '2024-01-01';
```

HHI = `10000 × SUM(share_of_reported_partners²)`；有效夥伴數 = `10000 / HHI`。描述的是回傳夥伴的集中度，不是完整市場集中度。`PROVISIONAL_VALUE_MATCH` 只表示數值相符；`NOT_CALCULABLE` 不應轉成零。

若需追查數值，在具 lineage 欄位的 Mart／fact 查出 batch、query key 與 revision，再對照 Raw 與 `ingestion_runs`。集中度為 scope 聚合，不應假設每列只對應單一來源紀錄。
