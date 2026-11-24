/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https:*www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

{# Return the catalog the current model lives in: model-level
   `{{ config(catalog=...) }}` wins, otherwise use profile `target.catalog`
   (which defaults to `default_catalog`). #}
{% macro starrocks__catalog_for() -%}
  {{ return(config.get('catalog', target.catalog)) }}
{%- endmacro %}

{# True when the current model lives in StarRocks's internal catalog
   (`default_catalog`). External catalogs (Iceberg, Hive, JDBC, ...) often
   need a different code path. #}
{% macro starrocks__is_internal_catalog() -%}
  {{ return(starrocks__catalog_for() == 'default_catalog') }}
{%- endmacro %}

{# Normalize a relation's `database` component to an external catalog name, or
   none when it addresses the internal catalog. Relations in `default_catalog`
   carry `database = none` (see `starrocks__generate_database_name`), so a name
   here means the relation lives outside it and must be addressed explicitly. #}
{% macro starrocks__external_catalog(database) -%}
  {%- if database is not none and database != 'default_catalog' -%}
    {{ return(database) }}
  {%- endif -%}
  {{ return(none) }}
{%- endmacro %}

{# The relation the current model materializes into, typed as `type`. For an
   external catalog it is `catalog`.`database`.`identifier`, where `database`
   is the model's `database` config, falling back to its schema. `this`
   normally carries both already (see `starrocks__generate_database_name` and
   `starrocks__generate_schema_name`); building it here keeps the target right
   when a project overrides `generate_schema_name` without honouring `database`. #}
{% macro starrocks__model_relation(type) -%}
  {%- set relation = this.incorporate(type=type) -%}
  {%- if starrocks__is_internal_catalog() -%}
    {{ return(relation) }}
  {%- endif -%}

  {%- set catalog = starrocks__catalog_for() -%}
  {%- if not catalog -%}
    {{ exceptions.raise_compiler_error("Model '" ~ model.name ~ "' has an empty 'catalog' config") }}
  {%- endif -%}
  {{ return(adapter.Relation.create(
      database=catalog,
      schema=config.get('database') or relation.schema,
      identifier=relation.identifier,
      type=type
  )) }}
{%- endmacro %}

{# Load the existing relation at `relation`, or none if it is absent. External
   catalogs list views in `information_schema.tables` as 'BASE TABLE' (Iceberg
   does, and leaves `information_schema.views` empty), so a view there comes
   back typed as a table and would be dropped with `DROP TABLE`, which fails.
   `SHOW CREATE TABLE` names its first column `View` for a view, so use that to
   correct the type. #}
{% macro starrocks__load_model_relation(relation) -%}
  {%- set existing = load_cached_relation(relation) -%}
  {%- if existing is none or starrocks__external_catalog(existing.database) is none or not existing.is_table -%}
    {{ return(existing) }}
  {%- endif -%}

  {%- set result = run_query("show create table " ~ existing) -%}
  {%- if result.column_names[0] | lower == 'view' -%}
    {{ return(existing.incorporate(type='view')) }}
  {%- endif -%}
  {{ return(existing) }}
{%- endmacro %}
