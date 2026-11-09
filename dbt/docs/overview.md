
{% docs __overview__ %}

# Trade Analytics — dbt Data Models

## Overview

This dbt project transforms validated UN Comtrade monthly
import records into analytical models hosted in BigQuery.

The current ingestion pipeline uses the UN Comtrade
Preview API and provides BOUNDED_PREVIEW source assurance.

## Data Architecture

1. Raw Source
   - trade_raw.comtrade_records

2. Staging
   - stg_comtrade_world_total
   - stg_comtrade_partner_detail

3. Intermediate
   - int_comtrade_scope_reconciliation
   - int_comtrade_partner_share
   - int_comtrade_concentration
   - int_comtrade_scope_quality

4. Core
   - dim_reporter
   - dim_partner
   - dim_commodity
   - dim_period
   - fct_trade_imports

5. Analytical Marts
   - mart_import_trends
   - mart_partner_shares
   - mart_product_rankings
   - mart_trade_concentration

## Data Grain

World Total records represent aggregate imports reported
for partner code 0.

Partner Detail records represent imports attributed to
individual trading partner codes.

World Total and Partner Detail must never be summed
together as independent trade values.

## Metric Definitions

### Import Growth

MoM and YoY are calculated only when the exact prior
calendar month or prior-year month exists.

Missing periods and zero baselines produce NULL growth
rates rather than misleading values.

### Partner Share

Share of World:
partner_import_value / world_total_value

Share of Reported Partners:
partner_import_value / partner_total_value

These metrics use different denominators and must not
be treated as equivalent.

### HHI

HHI = 10000 * SUM(reported_partner_share ^ 2)

The current HHI describes the concentration of returned
partner observations only.

Effective Partner Count = 10000 / HHI

## Data Quality Limitations

- BOUNDED_PREVIEW does not guarantee complete coverage.
- A numerical reconciliation match does not establish
  source completeness.
- Missing partner records do not imply zero imports.
- Commodity rankings cover observed commodities only.
- Historical data may not contain consecutive months.
- The current Raw ingestion implementation supports
  initial loads rather than historical revision replacement.

## Validation Strategy

Python Data Contracts validate source responses before
warehouse ingestion.

BigQuery load guards verify persisted Raw records.

dbt Data Tests validate the current warehouse state,
grain integrity and model relationships.

dbt Unit Tests validate analytical SQL using controlled,
known-answer fixtures.

{% enddocs %}
