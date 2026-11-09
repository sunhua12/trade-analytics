
{% macro verify_warehouse_setup() %}

    {% if target.name != 'dev' %}
        {{ exceptions.raise_compiler_error(
            "Stage 8.1 必須使用 dev Target"
        ) }}
    {% endif %}

    {% if target.schema != 'trade_analytics_dev' %}
        {{ exceptions.raise_compiler_error(
            "dbt Target Dataset 不正確：" ~ target.schema
        ) }}
    {% endif %}

    {% if execute %}

        {% set raw_relation = adapter.get_relation(
            database=target.database,
            schema='trade_raw',
            identifier='comtrade_records'
        ) %}

        {% if raw_relation is none %}

            {{ exceptions.raise_compiler_error(
                "找不到 Raw Table：trade_raw.comtrade_records"
            ) }}

        {% endif %}

        {% do log(
            "BigQuery Connection 驗證成功",
            info=True
        ) %}

        {% do log(
            "dbt Target：" ~ target.name,
            info=True
        ) %}

        {% do log(
            "Development Dataset：" ~ target.schema,
            info=True
        ) %}

        {% do log(
            "Raw Source：" ~ raw_relation,
            info=True
        ) %}

    {% endif %}

{% endmacro %}

