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
                            INSERT INTO {{ target_relation }}
                            SELECT * FROM {{ temp_relation }}
                    {% endcall %}

                    {#- Drop temp view after insert -#}
                    {{ adapter.drop_relation(temp_relation) }}
            {% else %}
                    {#- Columns do not match: drop and recreate -#}
                    {{ log("Table " ~ target_relation ~ " exists but columns do not match. Dropping and recreating table.", info=log_to_stdout) }}
                    {{ adapter.drop_relation(target_relation) }}
                    {% set tmp_vw_relation = target_relation.incorporate(path={"identifier": target_relation.identifier ~ '__dbt_tmp_vw'}, type='view') %}
                    {% call statement('main') %}
                            {{ create_table_as(False, target_relation, sql) }}
                    {% endcall %}
                    {#- Clean up temporary view created by create_table_as -#}
                    {{ adapter.drop_relation(tmp_vw_relation) }}
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