
{% macro comtrade_surrogate_key(fields) -%}

    to_hex(
        sha256(
            to_json_string(
                struct(
                    {% for field in fields %}
                        {{ field }} as key_{{ loop.index }}
                        {% if not loop.last %},{% endif %}
                    {% endfor %}
                )
            )
        )
    )

{%- endmacro %}
