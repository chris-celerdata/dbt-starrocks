{% macro starrocks__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {%- call statement('create_intermediate', fetch_result=False) -%}
        {{ starrocks__get_create_materialized_view_as_sql(intermediate_relation, sql) }}
    {%- endcall -%}
    alter materialized view `{{ intermediate_relation.identifier }}` swap with `{{ existing_relation.identifier }}`
{%- endmacro %}

{% macro starrocks__drop_materialized_view(relation) -%}
    drop materialized view if exists {{ relation }};
{%- endmacro %}

{% macro starrocks__get_create_materialized_view_as_sql(relation, sql) %}

    {%- set partition_by = config.get('partition_by') -%}
    {%- set buckets = config.get('buckets') -%}
    {%- set distributed_by = config.get('distributed_by') -%}
    {%- set properties = config.get('properties') -%}
    {%- set refresh_method = config.get('refresh_method', 'manual') -%}

    create materialized view {{ relation }}

    {%- if partition_by is not none -%}
        PARTITION BY (
        {%- for col in partition_by -%}
         {{ col }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
        )
    {%- endif -%}

    {%- if distributed_by is not none %}
    DISTRIBUTED BY HASH (
      {%- for item in distributed_by -%}
        {{ item }} {%- if not loop.last -%}, {%- endif -%}
      {%- endfor -%} )
      {%- if buckets is not none %}
        BUCKETS {{ buckets }}
      {% endif -%}
    {%- elif adapter.is_before_version("3.1.0") -%}
      {%- set msg -%}
        [distributed_by] must set before version 3.1, current version is {{ adapter.current_version() }}
      {%- endset -%}
      {{ exceptions.raise_compiler_error(msg) }}
    {% endif -%}
    refresh {{ refresh_method }}
    {% if properties is not none %}
    PROPERTIES (
      {% for key, value in properties.items() %}
        "{{ key }}" = "{{ value }}"{% if not loop.last %},{% endif %}
      {% endfor %}
    )
    {% endif %}
    as
    {{ sql }};

{% endmacro %}

{% macro starrocks__get_drop_relation_sql(relation) %}
    {% call statement(name="main") %}
        {%- if relation.is_materialized_view -%}
            {{ starrocks__drop_materialized_view(relation) }}
        {%- else -%}
            drop {{ relation.type }} if exists {{ relation }};
        {%- endif -%}
    {% endcall %}
{% endmacro %}

{% macro starrocks__refresh_materialized_view(relation) %}
  {%- if config.get('force_refresh', false) -%}
    refresh materialized view {{ relation }} with sync mode
  {%- else -%}
    {{ return("") }}
  {%- endif -%}
{% endmacro %}

{% macro starrocks__get_materialized_view_configuration_changes(existing_relation, new_config) %}
  {#
    Returns none when no changes are detected (dbt takes the no-op refresh path).
    Returns a non-empty dict when changes are detected describing changes. 
  #}
  {%- set mv_query -%}
    select is_active                  as is_active,
           materialized_view_definition as mv_def,
           refresh_type               as refresh_type
    from information_schema.materialized_views
    where table_schema = '{{ existing_relation.schema }}'
      and table_name   = '{{ existing_relation.table }}'
  {%- endset -%}
  {%- set mv_info = run_query(mv_query) -%}

  {%- if mv_info.rows | length == 0 -%}
    {{ return(none) }}
  {%- endif -%}

  {%- set is_active        = mv_info[0]['is_active'] -%}
  {%- set existing_def     = mv_info[0]['mv_def'] | trim -%}
  {%- set existing_refresh = mv_info[0]['refresh_type'] | lower -%}

  {%- if is_active == 'false' -%}
    {%- call statement('reactivate_' ~ existing_relation.identifier, fetch_result=False) -%}
      alter materialized view {{ existing_relation }} active
    {%- endcall -%}
  {%- endif -%}

  {%- set changes = {} -%}

  {%- if adapter.is_before_version("4.0.2") -%}
    {# See https://github.com/StarRocks/starrocks/pull/64318 released in 4.0.2 #}
    {%- do changes.update({'sql': true}) -%}
  {%- else -%}
    {%- set new_refresh = config.get('refresh_method', 'manual') | lower -%}
    {%- if existing_refresh != new_refresh -%}
      {%- do changes.update({'refresh_method': new_refresh}) -%}
    {%- endif -%}

    {%- set def_lower = existing_def.lower() -%}
    {%- set as_pos = def_lower.find('\nas ') if def_lower.find('\nas ') >= 0 else def_lower.find('\nas\n') -%}
    {%- if as_pos >= 0 -%}
      {%- set stored_raw = existing_def[as_pos + 4:] | trim -%}
      {%- set stored_sql = stored_raw[:-1] | trim if stored_raw.endswith(';') else stored_raw -%}
      {%- set new_raw    = sql | trim -%}
      {%- set new_sql    = new_raw[:-1] | trim if new_raw.endswith(';') else new_raw -%}
      {%- if starrocks__normalize_sql(stored_sql) != starrocks__normalize_sql(new_sql) -%}
        {%- do changes.update({'sql': true}) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}

  {%- if changes | length > 0 -%}
    {{ return(changes) }}
  {%- endif -%}

  {{ return(none) }}

{% endmacro %}

{% macro starrocks__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}
    {%- if configuration_changes.get('sql') -%}
        {{ starrocks__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) }}
    {%- else -%}
        {# Only refresh_method changed: ALTER in-place #}
        alter materialized view {{ relation }} refresh {{ configuration_changes['refresh_method'] }}
    {%- endif -%}
{% endmacro %}