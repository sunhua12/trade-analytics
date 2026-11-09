
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

echo "=== DBT VALIDATION PASSED ==="
