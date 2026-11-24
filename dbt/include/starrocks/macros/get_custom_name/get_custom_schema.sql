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

{# In an external catalog, a model's `database` config names the StarRocks
   database it is built in, which is dbt's schema, so it replaces the schema
   dbt would otherwise generate. Everywhere else dbt's default applies.

   A project that overrides `generate_schema_name` replaces this macro, and
   then has to apply the `database` config itself for `ref()` to see it. #}
{% macro starrocks__generate_schema_name(custom_schema_name=none, node=none) -%}
  {%- if node is not none
        and starrocks__external_catalog(starrocks__node_catalog(node)) is not none
        and node.config.get('database') -%}
    {{ return(node.config.get('database') | trim) }}
  {%- endif -%}
  {{ return(default__generate_schema_name(custom_schema_name, node)) }}
{%- endmacro %}
