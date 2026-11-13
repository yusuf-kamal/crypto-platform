{#
  dbt's default behaviour is to *append* a custom schema to the target one,
  so a model configured with schema='ops' would land in `gold_ops`. The ops
  schema already exists (sql/05) and the pipeline writes to it by name, so
  the custom schema has to be taken literally.

  This is dbt's own documented "custom schema, used as-is" override.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
