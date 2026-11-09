
#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd
)"

DBT_DIR="${PROJECT_ROOT}/dbt"

if [[ -z "${GCP_PROJECT_ID:-}" ]]; then
  echo "ERROR: GCP_PROJECT_ID is required." >&2
  exit 1
fi

if ! command -v dbt >/dev/null 2>&1; then
  echo "ERROR: dbt CLI was not found." >&2
  exit 1
fi

cd "${DBT_DIR}"

echo "=== 1. dbt Version ==="
dbt --version

echo "=== 2. BigQuery Connection ==="
dbt debug --target dev

echo "=== 3. Project Parse ==="
dbt parse --target dev

echo "=== 4. Controlled Unit Tests ==="
dbt test \
  --select "test_type:unit" \
  --target dev

echo "=== 5. Full dbt Build ==="
dbt build \
  --target dev

echo "=== 6. Full Data Quality Regression ==="
dbt test \
  --select "test_type:data" \
  --target dev

echo "=== 7. Documentation ==="
dbt docs generate --target dev

echo "=== STAGE 8.6 VALIDATION PASSED ==="
```

在專案根目錄執行：

```bash
chmod +x scripts/validate_dbt.sh

export GCP_PROJECT_ID="trade-analytics-508604"

source .venv-dbt/bin/activate

./scripts/validate_dbt.sh
```

Script 使用 `set -euo pipefail`，任何一個驗收步驟失敗，就會停止，不會繼續輸出成功訊息。

**成本提醒：** 此 Script 會查詢 BigQuery，且 `dbt build` 會重新建立部分實體 Tables。請在開發環境執行，避免對正式 Analytics Dataset 造成影響。

---

## 十一、補齊 YoY 的已知答案測試

最後，在 `dbt/models/marts/_marts_unit_tests.yml` 原本的 `unit_tests:` 清單末尾追加這個案例。

以 2024 年 3 月的 150 和 2025 年 3 月的 180 為例：

\[
YoY=\frac{180-150}{150}=0.2=20\%
\]

```yaml
  - name: unit_import_trends_year_over_year
    model: mart_import_trends
    description: >
      March 2025 must compare against March 2024,
      producing a 20 percent YoY growth rate.

    given:
      - input: ref('fct_trade_imports')
        rows:
          - trade_observation_id: trade-202403
            period_key: 202403
            period_start_date: "2024-03-01"
            period: "202403"
            reporter_code: 842
            partner_code: 0
            commodity_key: commodity-A
            cmd_code: "854231"
            query_type: world_total
            flow_code: M
            classification_code: H6
            hs_version: null
            freq_code: M
            partner2_code: 0
            customs_code: C00
            mot_code: 0
            is_original_classification: true
            import_value: 150

          - trade_observation_id: trade-202503
            period_key: 202503
            period_start_date: "2025-03-01"
            period: "202503"
            reporter_code: 842
            partner_code: 0
            commodity_key: commodity-A
            cmd_code: "854231"
            query_type: world_total
            flow_code: M
            classification_code: H6
            hs_version: null
            freq_code: M
            partner2_code: 0
            customs_code: C00
            mot_code: 0
            is_original_classification: true
            import_value: 180

    expect:
      rows:
        - trade_observation_id: trade-202403
          yoy_status: NO_PRIOR_YEAR
          yoy_growth_rate: null

        - trade_observation_id: trade-202503
          yoy_status: COMPARABLE
          previous_year_value: 150
          yoy_absolute_change: 30
          yoy_growth_rate: 0.2
          mom_status: NO_PRIOR_MONTH
```

## 十二、完整執行順序

<box border radius="lg" padding={3} gap={2}>
  **Stage 8.6 驗收指令**

  <text color="secondary" size="xs">請從專案根目錄執行。</text>
  <CodeBlock language="bash" editable>
source .venv-dbt/bin/activate
export GCP_PROJECT_ID="trade-analytics-508604"

cd dbt

# 1. 驗證設定與模型解析
dbt debug --target dev
dbt parse --target dev

# 2. 驗證受控案例
dbt test --select "test_type:unit" --target dev

# 3. 完整建構與測試
dbt build --target dev

# 4. 確認所有 Data Tests
dbt test --select "test_type:data" --target dev

# 5. 產生文件
dbt docs generate --target dev
