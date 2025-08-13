{%- set start_date = var('dates_start_date', '2025-01-01') -%}
{%- set end_datetime = run_started_at + modules.datetime.timedelta(days=30) -%}
{%- set end_date = end_datetime.strftime("%Y-%m-%d") -%}

WITH
    integer_expansion AS (
        SELECT
            ones.int_val
            + (10 * tens.int_val)
            + (100 * hundreds.int_val)
            + (1000 * thousands.int_val)
            + (10000 * ten_thousands.int_val) AS int_range
        FROM {{ ref('ints') }} AS ones
        CROSS APPLY {{ ref('ints') }} AS tens
        CROSS APPLY {{ ref('ints') }} AS hundreds
        CROSS APPLY {{ ref('ints') }} AS thousands
        CROSS APPLY {{ ref('ints') }} AS ten_thousands
    )

SELECT 
    CAST(NULLIF(DATEADD(DAY, y.int_range, '{{ start_date }}'), '1900-01-01') AS DATE) AS date_sid
FROM integer_expansion AS y
WHERE y.int_range <= DATEDIFF(DAY, '{{ start_date }}', '{{ end_date }}')
