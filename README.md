# Custom Table Materialization for Microsoft Fabric in dbt

This project contains a **custom dbt materialization** that is specifically designed to work with **Microsoft Fabric** environments, addressing an important limitation with Fabric’s handling of semantic models and underlying table storage after `DROP` and `CTAS` (Create Table As Select) operations.

---

## 1. How the Custom Materialization Works

The custom table materialization in [`macros/materializations/table.sql`](macros/materializations/table.sql) modifies the default dbt `table` behavior by:

- Ensuring **column consistency** between the existing target table and the new compiled SQL output using the [`check_column_consistency`](macros/check_column_consistency.sql) macro.
- Avoiding unnecessary `DROP TABLE` / `CREATE TABLE` cycles if the schema has not changed, thus reducing disruption to downstream Fabric semantic models.
- Handling **Fabric-specific quirks** where after a `DROP` + `CTAS`, the semantic model often fails to register the new underlying table location.
- Optionally replacing the destructive refresh with an **overwrite-in-place pattern** or intermediate rename to minimize metadata breakages.

**Key steps performed:**
1. Check if the table already exists.
2. Compare the existing schema to the new model schema.
3. If schema matches:
   - Overwrite table data in place.
4. If schema differs:
   - Use a safe refresh approach to avoid breaking table references in Fabric.
   
---

## 2. How to Apply the Custom Materialization to Other dbt Projects

To integrate this materialization into another dbt project:

1. **Copy the macros**  
   Copy the entire `macros/materializations/` directory and `macros/check_column_consistency.sql` into your dbt project’s `macros/` folder.

2. **Reference the materialization**  
   In your model’s `.sql` file, set the materialization:
   ```jinja
   {{ config(materialized='table', persist_docs={'relation': true, 'columns': true}) }}
   ```
   This will now use the custom materialization instead of dbt’s default.

3. **Adjust for your database**  
   The provided version is tailored for Microsoft Fabric (T-SQL). If using with another database, update SQL syntax in the macros accordingly.

4. **Optional settings**  
   You can enhance schema checks, add logging, or configure runtime options by modifying the macros.

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

### Example Usage

```sql
-- models/my_table.sql
{{ config(materialized='table') }}

SELECT
    id,
    name,
    created_at
FROM {{ ref('source_data') }}
```

When you run:
```bash
dbt run --select my_table
```
It will now apply the **Fabric-friendly** table replacement logic.

---

## Resources
- [dbt Documentation](https://docs.getdbt.com/docs/introduction)
- [Microsoft Fabric Overview](https://learn.microsoft.com/en-us/fabric/)
- [Power BI Semantic Models](https://learn.microsoft.com/en-us/power-bi/connect-data/semanticmodels)
