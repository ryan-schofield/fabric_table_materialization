# Custom Table Materialization for Microsoft Fabric in dbt

This project contains a **custom dbt materialization** that is specifically designed to work with **Microsoft Fabric** environments, addressing an important limitation with Fabric’s handling of semantic models and underlying table storage after `DROP` and `CTAS` (Create Table As Select) operations.

---

## 1. How the Custom Materialization Works

The custom table materialization in [`macros/materializations/table.sql`](macros/materializations/table.sql) modifies the default dbt `table` behavior by:

- Ensuring **column consistency** between the existing target table and the new compiled SQL output using the [`check_column_consistency`](macros/check_column_consistency.sql) macro.
- Avoiding unnecessary `DROP TABLE` / `CREATE TABLE` cycles if the schema has not changed, thus reducing disruption to downstream Fabric semantic models.
- Handling **Fabric-specific quirks** where after a `DROP` + `CTAS`, the semantic model often fails to register the new underlying table location.

**Key steps performed:**
1. Check if the table already exists.
2. Compare the existing schema to the new model schema.
3. If schema matches:
   - Overwrite table data in place.
4. If schema differs:
   - Fall back to default behavior (drop and recreate).
   
---

## 2. How to Apply the Custom Materialization to Other dbt Projects

To integrate this materialization into another dbt project:

1. **Copy the macros**
   Copy the entire `macros/materializations/` directory and `macros/check_column_consistency.sql` into your dbt project's `macros/` folder.

2. **Configure the materialization**
   You can apply this custom materialization at different levels:

### Model-Level Configuration
   In your model's `.sql` file, set the materialization:
   ```jinja
   {{ config(materialized='table', persist_docs={'relation': true, 'columns': true}) }}
   ```

### Project-Level Configuration
   Set the default materialization for all models in your `dbt_project.yml`:
   ```yaml
   models:
     your_project_name:
       +materialized: table  # Uses custom table materialization for all models
   ```

### Schema-Level Configuration
   Apply the materialization to specific schemas or model directories:
   ```yaml
   models:
     your_project_name:
       staging:
         +materialized: view  # Keep staging as views
       intermediate:
         +materialized: table  # Use custom table materialization
         +schema: intermediate
       marts:
         +materialized: table  # Use custom table materialization
         +schema: marts
   ```

3. **Using Named Materializations (Alternative Approach)**
   If you want to use the custom materialization alongside dbt's default table materialization, you can rename it:

   a. **Rename the materialization** in `macros/materializations/table.sql`:
   ```jinja
   {% materialization fabric_table, adapter='fabric' %}
   ```

   b. **Use the named materialization** in your models:
   ```jinja
   {{ config(materialized='fabric_table') }}
   ```

   c. **Configure in dbt_project.yml** for specific use cases:
   ```yaml
   models:
     your_project_name:
       critical_tables:
         +materialized: fabric_table  # Use Fabric-optimized materialization
       regular_tables:
         +materialized: table  # Use dbt's default table materialization
   ```

4. **Optional settings**
   You can enhance schema checks, add logging, or configure runtime options by modifying the macros.

### Logging Configuration

The custom materialization supports a `log_to_stdout` configuration option that controls whether logging messages from the materialization process are sent to stdout. This can be useful for debugging and monitoring table creation and update operations.

**Default behavior**: `log_to_stdout` defaults to `false`, meaning logs are only visible in dbt's standard logging output.

#### Model-Level Logging Configuration
```jinja
{{ config(
    materialized='table',
    meta={'log_to_stdout': true}
) }}
```

#### Schema-Level Logging Configuration
```yaml
# dbt_project.yml
models:
  your_project_name:
    marts:
      +materialized: table
      +meta:
        log_to_stdout: true  # Enable stdout logging for all marts models
```

#### Project-Level Logging Configuration
```yaml
# dbt_project.yml
models:
  your_project_name:
    +materialized: table
    +meta:
      log_to_stdout: true  # Enable stdout logging for all models using table materialization
```

When `log_to_stdout` is enabled, you'll see detailed messages about:
- Whether a new table is being created
- Whether an existing table's columns match and truncate/insert is being used
- Whether a table is being dropped and recreated due to column mismatches
- When relations are being dropped (e.g., views being replaced with tables)

---

> **Note:** This is an **early implementation** of the custom materialization.
> It has **not been tested against every possible SQL query structure** that may be valid in a dbt model.
> Complex or highly specialized SQL patterns may require additional macro adjustments.

## 3. Why Use This Custom Materialization

In **Microsoft Fabric**, when a semantic model references a table and you use dbt’s default `table` materialization:
- dbt **drops** the table and recreates it using `CTAS`.
- This changes the **underlying physical file location** in Fabric.
- Semantic models often **fail** to detect the new file location, leading to broken dashboards and refresh errors.

By using this custom materialization:
- You **reduce refresh disruptions** for Power BI and Fabric semantic models.
- Your data pipeline becomes **more stable** by preserving table references and metadata.
- You can still detect and adjust for schema changes, but in a **controlled** manner.

---

## 4. Configuration Examples and Best Practices

### Basic Model Usage
```sql
-- models/my_table.sql
{{ config(materialized='table') }}

SELECT
    id,
    name,
    created_at
FROM {{ ref('source_data') }}
```

### Advanced Project Configuration Example
```yaml
# dbt_project.yml
models:
  your_project_name:
    # Default to views for development speed
    +materialized: view
    
    staging:
      # Keep staging lightweight
      +materialized: view
      +schema: staging
    
    intermediate:
      # Use custom table materialization for intermediate processing
      +materialized: table
      +schema: intermediate
    
    marts:
      # Critical business tables use Fabric-optimized materialization
      +materialized: table
      +schema: marts
      
    # Specific high-volume tables that need the optimization
    critical_models:
      +materialized: table
      +schema: critical
```

### Using Named Materialization for Selective Application
If you rename the materialization to `fabric_table`, you can selectively apply it:

```yaml
# dbt_project.yml
models:
  your_project_name:
    # Most models use standard dbt table materialization
    +materialized: table
    
    # Only specific models that have Fabric semantic model dependencies
    fabric_dependent:
      +materialized: fabric_table  # Uses the custom Fabric-optimized logic
      +schema: semantic_models
```

```sql
-- models/fabric_dependent/customer_summary.sql
{{ config(
    materialized='fabric_table',
    persist_docs={'relation': true, 'columns': true},
    meta={'log_to_stdout': true}  -- Enable detailed logging for this critical table
) }}

SELECT
    customer_id,
    total_orders,
    lifetime_value
FROM {{ ref('customer_orders') }}
```

### Environment-Specific Configuration
```yaml
# dbt_project.yml
models:
  your_project_name:
    +materialized: "{{ 'table' if target.name == 'prod' else 'view' }}"
    # Enable logging in development for debugging
    +meta:
      log_to_stdout: "{{ true if target.name == 'dev' else false }}"
    
    # Critical production tables always use Fabric optimization
    critical_tables:
      +materialized: "{{ 'fabric_table' if target.name == 'prod' else 'table' }}"
      # Always log critical table operations in production
      +meta:
        log_to_stdout: "{{ true if target.name == 'prod' else false }}"
```

### Running the Models
When you run:
```bash
dbt run --select my_table
```
It will apply the **Fabric-friendly** table replacement logic based on your configuration.

For selective runs with named materialization:
```bash
# Run only models using the custom Fabric materialization
dbt run --select config.materialized:fabric_table

# Run all table materializations (both default and custom)
dbt run --select config.materialized:table,config.materialized:fabric_table
```

---

## 5. Troubleshooting and Considerations

### When to Use Project vs Schema vs Model Level Configuration

**Project Level (`+materialized: table`)**
- ✅ Use when most of your models benefit from the Fabric optimization
- ✅ Simplifies configuration management
- ⚠️ May impact development speed if applied to all models including staging

**Schema Level**
- ✅ Best for organizing by data layer (staging → intermediate → marts)
- ✅ Allows different strategies per layer
- ✅ Recommended approach for most projects

**Model Level**
- ✅ Use for exceptions to your general strategy
- ✅ Good for testing the materialization on specific models first
- ⚠️ Can become difficult to manage at scale

### Named Materialization Benefits

Using a renamed materialization (e.g., `fabric_table`) provides:
- **Flexibility**: Use both default dbt and custom Fabric logic in the same project
- **Gradual adoption**: Test on specific models before broader rollout
- **Clear intent**: Makes it obvious which models use Fabric-specific optimizations
- **Easier debugging**: Can easily identify which materialization logic is being used

### Common Issues and Solutions

**Issue**: Models still dropping and recreating tables
- **Solution**: Verify the materialization is correctly named and the macro files are in the right location
- **Check**: Run `dbt debug` to ensure macros are being loaded

**Issue**: Column consistency check failing unexpectedly
- **Solution**: Review the [`check_column_consistency`](macros/check_column_consistency.sql) macro for data type mapping issues
- **Check**: Ensure your SQL doesn't have complex CTEs that might confuse the column detection

**Issue**: Performance degradation with the custom materialization
- **Solution**: The truncate/insert approach may be slower for very large tables
- **Consider**: Using the named approach and applying selectively to tables with Fabric semantic model dependencies

### Migration Strategy

1. **Start small**: Use model-level configuration on a few critical tables
2. **Test thoroughly**: Verify semantic models continue working after dbt runs
3. **Expand gradually**: Move to schema-level configuration for broader adoption
4. **Monitor**: Watch for any performance or reliability issues

---

## Resources
- [dbt Documentation](https://docs.getdbt.com/docs/introduction)
- [Microsoft Fabric Overview](https://learn.microsoft.com/en-us/fabric/)
- [Power BI Semantic Models](https://learn.microsoft.com/en-us/power-bi/connect-data/semanticmodels)
- [dbt Materialization Configuration](https://docs.getdbt.com/reference/model-configs#materialized)
