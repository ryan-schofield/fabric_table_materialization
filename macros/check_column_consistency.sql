{% macro check_column_consistency(model_name, schema_name=none, model_compiled_sql=none) %}
  {# Construct the target relation using provided schema or default to target schema #}
  {%- set model_database = target.database -%}
  {%- set model_schema = schema_name or target.schema -%}
  {%- set model_alias = model_name -%}
  
  {%- set target_relation = api.Relation.create(
      identifier=model_alias,
      schema=model_schema,
      database=model_database,
      type='table'
  ) -%}
  
  {# Check if the table exists in the database #}
  {%- set existing_relation = adapter.get_relation(
      database=target_relation.database,
      schema=target_relation.schema,
      identifier=target_relation.identifier
  ) -%}
  
  {%- if existing_relation -%}
    {# Get columns from existing table #}
    {%- set existing_columns = adapter.get_columns_in_relation(existing_relation) -%}
    
    {%- set existing_column_info = [] -%}
    {%- for col in existing_columns -%}
      {%- do existing_column_info.append({
          'name': col.name,
          'dtype': col.dtype,
          'char_size': col.char_size,
          'numeric_precision': col.numeric_precision,
          'numeric_scale': col.numeric_scale
      }) -%}
    {%- endfor -%}
    
    {# Create a temporary view from the model's compiled SQL to get its columns #}
    {%- set temp_view_name = 'temp_column_check_' ~ model_name ~ '_' ~ modules.datetime.datetime.now().strftime('%Y%m%d_%H%M%S') -%}
    {%- set temp_relation = api.Relation.create(
        identifier=temp_view_name,
        schema=target_relation.schema,
        database=target_relation.database,
        type='view'
    ) -%}
    
    {# Get the model's compiled SQL by referencing the model directly #}
    {%- set compiled_sql -%}
      {%- if model_compiled_sql -%}
        {{ model_compiled_sql }}
      {%- else -%}
        SELECT * FROM {{ ref(model_name) }}
      {%- endif -%}
    {%- endset -%}
    
    {%- if compiled_sql -%}
      {# Fix for T-SQL syntax - remove extra parentheses #}
      {%- call statement('create_temp_view', fetch_result=false) -%}
        CREATE VIEW {{ temp_relation.schema }}.{{ temp_relation.name}} AS
          {{ compiled_sql }}
      {%- endcall -%}
    {%- else -%}
      {{ return({
          'columns_match': false,
          'existing_columns': [],
          'model_columns': [],
          'error': 'No SQL code found for model: ' ~ model_name
      }) }}
    {%- endif -%}
    
    {# Get columns from the model via temp view #}
    {%- set model_columns = adapter.get_columns_in_relation(temp_relation) -%}
    
    {%- set model_column_info = [] -%}
    {%- for col in model_columns -%}
      {%- do model_column_info.append({
          'name': col.name,
          'dtype': col.dtype,
          'char_size': col.char_size,
          'numeric_precision': col.numeric_precision,
          'numeric_scale': col.numeric_scale
      }) -%}
    {%- endfor -%}
    
    {# Clean up temporary view #}
    {%- call statement('drop_temp_view', fetch_result=false) -%}
      DROP VIEW {{ temp_relation.schema }}.{{ temp_relation.name}}
    {%- endcall -%}
    
    {# Compare columns #}
    {%- set existing_column_names = existing_column_info | map(attribute='name') | map('lower') | list -%}
    {%- set model_column_names = model_column_info | map(attribute='name') | map('lower') | list -%}
    
    {%- set columns_match = (existing_column_names | sort) == (model_column_names | sort) -%}
    
    {# Return detailed comparison result #}
    {%- set result = {
        'columns_match': columns_match,
        'existing_columns': existing_column_info,
        'model_columns': model_column_info
    } -%}
    
  {%- else -%}
    {%- set result = {
        'columns_match': false,
        'existing_columns': [],
        'model_columns': [],
        'error': 'Table does not exist - this is a new table creation'
    } -%}
  {%- endif -%}
  
  {{ return(result) }}

{% endmacro %}