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

{# The catalog a node lives in: its `catalog` config, else the profile's
   `catalog`. Shared by the database and schema name macros below, which run at
   parse time, before `config` is bound to the node. #}
{% macro starrocks__node_catalog(node) -%}
  {%- set catalog = node.config.get('catalog') if node is not none else none -%}
  {{ return(catalog or target.catalog or 'default_catalog') }}
{%- endmacro %}

{# dbt's `database` component carries the StarRocks catalog. Nodes in
   `default_catalog` get none, so they render as `schema`.`identifier`.
   A node in an external catalog gets that catalog, so `ref()` and `this`
   render as `catalog`.`database`.`identifier` wherever they are used.

   `custom_database_name` is ignored: a model's `database` config names the
   StarRocks database, which is dbt's schema (see `starrocks__generate_schema_name`). #}
{% macro starrocks__generate_database_name(custom_database_name=none, node=none) -%}
  {{ return(starrocks__external_catalog(starrocks__node_catalog(node))) }}
{%- endmacro %}
