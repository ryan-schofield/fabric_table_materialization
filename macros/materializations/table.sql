{% materialization table, adapter='fabric' %}

    {%- set target_relation = this.incorporate(type='table') %}
    {%- set existing_relation = adapter.get_relation(
        database=this.database,
        schema=this.schema,
        identifier=this.identifier
    ) -%}
    {%- set log_to_stdout = config.get('meta', {}).get('log_to_stdout', false) -%}
    
    {#- If an existing relation is a view, drop it so we can create a table in its place -#}
    {% if existing_relation is not none and not existing_relation.is_table %}
        {{ log("Dropping relation " ~ existing_relation ~ " because it is of type " ~ existing_relation.type, info=log_to_stdout) }}
        {{ adapter.drop_relation(existing_relation) }}
        {% set existing_relation = none %}
    {% endif %}

    {% set grant_config = config.get('grants') %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% if existing_relation is none %}
        {#- Table does not exist: create it using CTAS -#}
        {{ log("Creating new table " ~ target_relation ~ " using CREATE TABLE AS SELECT", info=log_to_stdout) }}
        {% set tmp_vw_relation = target_relation.incorporate(path={"identifier": target_relation.identifier ~ '__dbt_tmp_vw'}, type='view') %}
        {% call statement('main') %}
            {{ create_table_as(False, target_relation, sql) }}
        {% endcall %}
        {#- Clean up temporary view created by create_table_as -#}
        {{ adapter.drop_relation(tmp_vw_relation) }}
    {% else %}
        {#- Table exists: check column consistency -#}
        {% set consistency = check_column_consistency(this.identifier, this.schema, sql) %}

        {% if consistency['columns_match'] %}
            {#- Columns match: truncate and insert using temp view to handle CTEs and complex SQL -#}
            {{ log("Table " ~ target_relation ~ " exists with matching columns. Using truncate and insert strategy.", info=log_to_stdout) }}
            {% set temp_view_name = target_relation.identifier ~ '__dbt_tmp_insert' %}
            {% set temp_relation = api.Relation.create(
                identifier=temp_view_name,
                schema=target_relation.schema,
                database=target_relation.database,
                type='view'
            ) %}
            
            {{ adapter.drop_relation(temp_relation) }}

            {% call statement('create_temp_view') %}
                CREATE VIEW {{ temp_relation.schema ~ "." ~ temp_relation.identifier }} AS
                {{ sql }}
            {% endcall %}

            {% call statement('truncate_table') %}
                TRUNCATE TABLE {{ target_relation }}
            {% endcall %}

            {% call statement('main') %}
                {#- Build explicit column list from existing columns to maintain order -#}
                {%- set column_names = consistency['existing_columns'] | map(attribute='name') | list -%}
                {%- set column_list = column_names | join(', ') -%}
                INSERT INTO {{ target_relation }} ({{ column_list }})
                SELECT {{ column_list }} FROM {{ temp_relation }}
            {% endcall %}

            {#- Drop temp view after insert -#}
            {{ adapter.drop_relation(temp_relation) }}
        {% else %}
            {{ log("Table " ~ target_relation ~ " exists but columns do not match. Using truncate, alter, and insert strategy.", info=log_to_stdout) }}
            
            {#- Create temp view for inserting data -#}
            {% set temp_view_name = target_relation.identifier ~ '__dbt_tmp_alter' %}
            {% set temp_relation = api.Relation.create(
                identifier=temp_view_name,
                schema=target_relation.schema,
                database=target_relation.database,
                type='view'
            ) %}
            
            {{ adapter.drop_relation(temp_relation) }}

            {% call statement('create_temp_view_for_alter') %}
                CREATE VIEW {{ temp_relation.schema ~ "." ~ temp_relation.identifier }} AS
                {{ sql }}
            {% endcall %}

            {#- Step 1: Truncate the table -#}
            {% call statement('truncate_table_for_alter') %}
                TRUNCATE TABLE {{ target_relation }}
            {% endcall %}
            
            {#- Step 2: Drop columns that exist in table but not in model -#}
            {% if consistency['columns_to_drop'] | length > 0 %}
                {{ log("Dropping " ~ consistency['columns_to_drop'] | length ~ " columns", info=log_to_stdout) }}
                {% for column_name in consistency['columns_to_drop'] %}
                    {% call statement('drop_column_' ~ loop.index) %}
                        ALTER TABLE {{ target_relation }} DROP COLUMN [{{ column_name }}]
                    {% endcall %}
                    {{ log("Dropped column: " ~ column_name, info=log_to_stdout) }}
                {% endfor %}
            {% endif %}

            {#- Step 3: Add columns that exist in model but not in table -#}
            {% if consistency['columns_to_add'] | length > 0 %}
                {{ log("Adding " ~ consistency['columns_to_add'] | length ~ " columns", info=log_to_stdout) }}
                {% for column_info in consistency['columns_to_add'] %}
                    {% call statement('add_column_' ~ loop.index) %}
                        ALTER TABLE {{ target_relation }} ADD [{{ column_info.name }}] {{ column_info.full_definition }}
                    {% endcall %}
                    {{ log("Added column: " ~ column_info.name ~ " " ~ column_info.full_definition, info=log_to_stdout) }}
                {% endfor %}
            {% endif %}

            {#- Step 4: Calculate final table column order and build explicit SELECT -#}
            {%- set final_columns = [] -%}
            
            {#- Create lowercase version of columns_to_drop for comparison -#}
            {%- set columns_to_drop_lower = consistency['columns_to_drop'] | map('lower') | list -%}
            
            {#- Add existing columns (minus dropped ones) in their original order -#}
            {%- for existing_col in consistency['existing_columns'] -%}
                {%- if existing_col.name | lower not in columns_to_drop_lower -%}
                    {%- do final_columns.append(existing_col.name) -%}
                {%- endif -%}
            {%- endfor -%}
            
            {#- Add new columns at the end -#}
            {%- for new_col in consistency['columns_to_add'] -%}
                {%- do final_columns.append(new_col.name) -%}
            {%- endfor -%}

            {#- Step 5: Insert data with explicit column list matching table order -#}
            {% if final_columns | length > 0 %}
                {% set column_list = final_columns | join(', ') %}
                {{ log("Inserting data with column order: " ~ column_list, info=log_to_stdout) }}
                {% call statement('main') %}
                    INSERT INTO {{ target_relation }} ({{ column_list }})
                    SELECT {{ column_list }} FROM {{ temp_relation }}
                {% endcall %}
            {% else %}
                {{ exceptions.raise_compiler_error("No columns remain after ALTER operations. This should not happen.") }}
            {% endif %}

            {#- Drop temp view after insert -#}
            {{ adapter.drop_relation(temp_relation) }}
        {% endif %}
    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
    {% do persist_docs(target_relation, model) %}
    {{ adapter.commit() }}

    {#- Add model constraints after data load -#}
    {{ build_model_constraints(target_relation) }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}