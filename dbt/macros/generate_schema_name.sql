{#
    Por padrão o dbt concatena o schema do profile com o schema customizado
    (ex.: public_staging). Esta macro usa o schema customizado como está,
    resultando nos schemas staging, intermediate e marts.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
