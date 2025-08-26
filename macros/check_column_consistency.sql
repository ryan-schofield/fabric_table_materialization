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

    {%- set columns_to_drop = [] -%}
    {%- set columns_to_add = [] -%}
    
    {# Return detailed comparison result #}
    {%- for existing_col in existing_column_info -%}
      {%- if existing_col.name | lower not in model_column_names -%}
        {%- do columns_to_drop.append(existing_col.name) -%}
      {%- endif -%}
    {%- endfor -%}

    {%- for model_col in model_column_info -%}
      {%- if model_col.name | lower not in existing_column_names -%}
        {# Build complete column definition for ALTER TABLE ADD #}
        {%- set base_dtype = model_col.dtype or 'VARCHAR' -%}
        {%- set full_column_definition = base_dtype -%}
        
        {# Handle character types with size specifications #}
        {%- if base_dtype.upper() in ('VARCHAR', 'CHAR', 'NVARCHAR', 'NCHAR') and model_col.char_size -%}
          {%- set full_column_definition = base_dtype ~ '(' ~ model_col.char_size ~ ')' -%}
        {%- elif base_dtype.upper() in ('VARCHAR', 'NVARCHAR') and not model_col.char_size -%}
          {%- set full_column_definition = base_dtype ~ '(8000)' -%}
        {%- elif base_dtype.upper() in ('CHAR', 'NCHAR') and not model_col.char_size -%}
          {%- set full_column_definition = base_dtype ~ '(1)' -%}
        {# Handle numeric types with precision and scale #}
        {%- elif base_dtype.upper() in ('DECIMAL', 'NUMERIC') and model_col.numeric_precision -%}
          {%- if model_col.numeric_scale -%}
            {%- set full_column_definition = base_dtype ~ '(' ~ model_col.numeric_precision ~ ',' ~ model_col.numeric_scale ~ ')' -%}
          {%- else -%}
            {%- set full_column_definition = base_dtype ~ '(' ~ model_col.numeric_precision ~ ')' -%}
          {%- endif -%}
        {%- elif base_dtype.upper() in ('FLOAT', 'REAL') and model_col.numeric_precision -%}
          {%- set full_column_definition = base_dtype ~ '(' ~ model_col.numeric_precision ~ ')' -%}
        {# Handle other data types that might have precision #}
        {%- elif base_dtype.upper() in ('TIME', 'DATETIME2', 'DATETIMEOFFSET') and model_col.numeric_scale -%}
          {%- set full_column_definition = base_dtype ~ '(' ~ model_col.numeric_scale ~ ')' -%}
        {%- endif -%}
        
        {%- do columns_to_add.append({
            'name': model_col.name,
            'dtype': base_dtype,
            'full_definition': full_column_definition,
            'char_size': model_col.char_size,
            'numeric_precision': model_col.numeric_precision,
            'numeric_scale': model_col.numeric_scale
        }) -%}
      {%- endif -%}
    {%- endfor -%}

    {%- set result = {
        'columns_match': columns_match,
        'existing_columns': existing_column_info,
        'model_columns': model_column_info,
        'columns_to_drop': columns_to_drop,
        'columns_to_add': columns_to_add
    } -%}
    {{ log("Columns to add: " ~ result.columns_to_add) }}
    {{ log("Columns to drop: " ~ result.columns_to_drop) }}
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