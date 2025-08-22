SELECT
    date_sid AS [date]
    , ISNULL(date_sid, '1900-01-01') AS date_sid
    , DATEPART(WEEKDAY, date_sid) AS day_of_week
    , CAST(FORMAT(date_sid, 'dddd') AS VARCHAR(10)) AS day_of_week_name
    , DATEPART(WEEK, date_sid) AS week_of_year
    , DATEPART(YEAR, date_sid) AS year_num
    , CAST(DATEPART(YEAR, date_sid) AS VARCHAR(4)) AS year_text
    , DATEPART(MONTH, date_sid) AS month_num
    , CAST(FORMAT(date_sid, 'MMMM') AS VARCHAR(10)) AS month_name
    , DATEPART(QUARTER, date_sid) AS [quarter]
    , DATEPART(DAY, date_sid) AS day_of_month
    , DATEPART(DAYOFYEAR, date_sid) AS day_of_year
    , DATETRUNC(MONTH, date_sid) AS month_start_date
    , EOMONTH(date_sid) AS month_end_date
    , DATETRUNC(YEAR, date_sid) AS year_start_date
    , DATEADD(DAY, -1, DATEADD(YEAR, 1, DATETRUNC(YEAR, date_sid))) AS year_end_date
FROM {{ ref('dates') }}
