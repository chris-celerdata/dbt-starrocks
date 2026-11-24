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

{% materialization table, adapter='starrocks' %}
  
  {%- set target_relation = starrocks__model_relation('table') -%}
  {%- set is_external = starrocks__external_catalog(target_relation.database) is not none -%}
  {%- set existing_relation = starrocks__load_model_relation(target_relation) -%}
  {%- set on_table_exists = config.get('on_table_exists', 'replace') -%}

  {%- if on_table_exists == 'swap' and is_external -%}
    {%- set msg -%}
      on_table_exists='swap' is only supported for tables in default_catalog: external catalogs have no ALTER TABLE ... SWAP WITH
    {%- endset %}
    {{ exceptions.raise_compiler_error(msg) }}
  {%- endif -%}

  {%- set sql = model['compiled_code'] -%}

  {{ run_hooks(pre_hooks) }}

  {%- if existing_relation is not none and existing_relation.is_table and on_table_exists == 'swap' -%}
    {%- set tmp_relation = make_temp_relation(target_relation).incorporate(type='table') -%}
    {{ log("Building " ~ tmp_relation ~ " and swapping it with " ~ target_relation, info=True) }}
    {{ adapter.drop_relation(tmp_relation) }}
    {% call statement('main') -%}
      {{ starrocks__create_table_as(false, tmp_relation, sql) }}
    {%- endcall %}
    {{ starrocks__exchange_relation(target_relation, tmp_relation) }}
    {{ adapter.drop_relation(tmp_relation) }}
    {{ run_hooks(post_hooks) }}
    {{ return({'relations': [target_relation]}) }}
  {%- endif -%}

  {%- if existing_relation is not none -%}
    {%- if should_full_refresh() or on_table_exists in ('replace', 'swap') -%}
      {{ log("Dropping and recreating table", info=True) }}
      {{ adapter.drop_relation(existing_relation) }}
      {%- set existing_relation = none -%}
      
    {%- elif on_table_exists == 'append' -%}
      {% call statement('main') -%}
        INSERT INTO {{ target_relation }}
        {{ sql }}
      {%- endcall %}
      {{ return({'relations': [target_relation]}) }}
      
    {%- elif on_table_exists == 'ignore' -%}
      {{ return({'relations': [target_relation]}) }}
      
    {%- else -%}
      {%- set msg -%}
        Unknown on_table_exists strategy: '{{ on_table_exists }}'.
        Valid options: 'replace', 'swap', 'append', 'ignore'
      {%- endset %}
      {{ exceptions.raise_compiler_error(msg) }}
    {%- endif -%}
  {%- endif -%}
  
  {%- if existing_relation is none -%}
    {{ log("Creating new table: " ~ target_relation, info=True) }}
    {% call statement('main') -%}
      {{ starrocks__create_table_as(false, target_relation, sql, is_external) }}
    {%- endcall %}
  {%- endif -%}
  
  {{ run_hooks(post_hooks) }}
  
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
