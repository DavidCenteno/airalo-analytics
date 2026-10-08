{#
  Drops the temporary schema (BigQuery dataset) a CI run built into,
  including all tables and views inside it.
  Refuses to run unless the target schema starts with "ci_",
  so it can never drop a dev or prod schema by accident.
#}
{% macro drop_ci_schema() %}
    {% set schema_name = target.schema %}
    {% if not schema_name.lower().startswith('ci_') %}
        {{ exceptions.raise_compiler_error("Refusing to drop non-CI schema: " ~ schema_name) }}
    {% endif %}

    {% set relation = api.Relation.create(database=target.database, schema=schema_name) %}
    {% do adapter.drop_schema(relation) %}
    {{ log("Dropped CI schema " ~ target.database ~ "." ~ schema_name, info=True) }}
{% endmacro %}
