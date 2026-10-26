/*
  Переопределение стандартного макроса генерации имени схемы.
  Отключает склейку схем (schema_profile + schema_project).
  Если в dbt_project.yml указан +schema, используется только он.
  Если нет — используется схема из profiles.yml.
*/
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}