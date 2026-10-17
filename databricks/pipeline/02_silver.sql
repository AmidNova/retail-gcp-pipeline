-- SILVER: typed, flagged, then restricted to the sales scope.
-- Port of the dbt models stg_retail__invoice_lines and
-- int_invoice_lines__customer_sales (include/dbt/models/).

-- 1. Every line, typed. Same rules as the dbt staging model: no row removed.
--
-- FAIL UPDATE on the parsing checks: if an id, a date, a quantity or a price
-- cannot be read, the file format changed and every number downstream would be
-- wrong, so the update stops.
-- WARN on guests and cancellations: they are valid lines; the share shows in the
-- pipeline's data quality tab (22.8 % and 1.8 % of lines in the BigQuery run).
CREATE OR REFRESH STREAMING TABLE retail.silver.invoice_lines (
  CONSTRAINT valid_invoice_id EXPECT (invoice_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_invoiced_at EXPECT (invoiced_at IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_quantity EXPECT (quantity IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_unit_price EXPECT (unit_price IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT has_customer_id EXPECT (customer_id IS NOT NULL),
  CONSTRAINT not_cancellation EXPECT (NOT is_cancellation)
)
COMMENT 'One row per raw invoice line: renamed, typed and flagged. No rows filtered.'
AS SELECT
  Invoice AS invoice_id,
  StockCode AS stock_code,
  trim(Description) AS description,
  try_cast(Quantity AS BIGINT) AS quantity,
  try_cast(Price AS DECIMAL(10, 3)) AS unit_price,
  try_to_timestamp(InvoiceDate, 'yyyy-MM-dd HH:mm:ss') AS invoiced_at,
  -- Customer ids may be exported as "13085.0": normalise to "13085"
  cast(try_cast(try_cast(nullif(trim(Customer_id), '') AS DOUBLE) AS BIGINT) AS STRING) AS customer_id,
  trim(Country) AS country_name,
  try_cast(Quantity AS BIGINT) * try_cast(Price AS DECIMAL(10, 3)) AS line_amount,
  Invoice LIKE 'C%' AS is_cancellation,
  source_file,
  ingested_at
FROM STREAM(retail.bronze.online_retail);

-- 2. Stock codes that are not products (postage, manual adjustments, fees,
-- vouchers...). Same reviewed CSV as the dbt seed, uploaded to the volume.
CREATE OR REFRESH MATERIALIZED VIEW retail.silver.non_merchandise_stock_codes (
  CONSTRAINT valid_stock_code EXPECT (stock_code IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Stock codes excluded from the sales scope, from include/dbt/seeds/non_merchandise_stock_codes.csv.'
AS SELECT
  stock_code,
  category AS non_merchandise_category
FROM read_files(
  '/Volumes/retail/bronze/landing/reference/non_merchandise_stock_codes.csv',
  format => 'csv',
  header => true,
  schema => 'stock_code STRING, category STRING'
);

-- 3. The sales scope: positive merchandise sales of identified customers.
--
-- DROP ROW on the scope rules: a return or a postage line is valid data, but it
-- is not a sale. Dropping keeps the pipeline running and counts what was
-- removed, rule by rule, in the data quality tab. FAIL would stop every run,
-- since these lines are always present.
--
-- WARN on exact duplicates (26,055 in-scope lines in BigQuery): kept, so the
-- totals match the BigQuery pipeline line for line, but measured. Nothing in
-- the data proves they are mistakes rather than an item scanned twice.
CREATE OR REFRESH MATERIALIZED VIEW retail.silver.sales_lines (
  CONSTRAINT positive_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW,
  CONSTRAINT positive_unit_price EXPECT (unit_price > 0) ON VIOLATION DROP ROW,
  CONSTRAINT identified_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT is_merchandise EXPECT (is_merchandise) ON VIOLATION DROP ROW,
  CONSTRAINT not_exact_duplicate EXPECT (NOT is_exact_duplicate)
)
COMMENT 'Positive merchandise sales lines of identified customers (the star schema scope in dbt).'
AS WITH flagged AS (
  SELECT
    lines.*,
    codes.stock_code IS NULL AS is_merchandise
  FROM retail.silver.invoice_lines AS lines
  LEFT JOIN (SELECT DISTINCT stock_code FROM retail.silver.non_merchandise_stock_codes) AS codes
    ON lines.stock_code = codes.stock_code
)
SELECT
  invoice_id,
  invoiced_at,
  cast(invoiced_at AS DATE) AS invoice_date,
  customer_id,
  country_name,
  stock_code,
  description,
  quantity,
  unit_price,
  line_amount,
  is_merchandise,
  -- Second and later copies of an identical in-scope line. Computed here
  -- because an expectation cannot contain a window function. Identical lines
  -- share their scope, so only in-scope copies are flagged.
  row_number() OVER (
    PARTITION BY invoice_id, stock_code, description, quantity, unit_price,
                 invoiced_at, customer_id, country_name
    ORDER BY ingested_at
  ) > 1
  AND quantity > 0 AND unit_price > 0 AND customer_id IS NOT NULL AND is_merchandise
    AS is_exact_duplicate
FROM flagged;
