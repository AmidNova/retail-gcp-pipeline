-- BRONZE: the CSV as it arrived, one row per line, every column kept as a string.
--
-- Auto Loader (read_files inside STREAM) only reads files it has not seen yet:
-- dropping a new CSV in the landing folder and refreshing the pipeline ingests
-- that file only. Strings everywhere means a malformed value never blocks the
-- ingestion; typing happens in silver, where it can be checked.
--
-- escape => '"': the export doubles quotes inside quoted fields
-- ("RECORD FRAME 7"" SINGLE SIZE"). Spark's default escape character is a
-- backslash, which would split those lines.

CREATE OR REFRESH STREAMING TABLE retail.bronze.online_retail
COMMENT 'Raw Online Retail II lines from the landing volume, all columns as strings.'
AS SELECT
  Invoice,
  StockCode,
  Description,
  Quantity,
  InvoiceDate,
  Price,
  Customer_id,
  Country,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/retail/bronze/landing/online_retail/',
  format => 'csv',
  header => true,
  escape => '"',
  schema => 'Invoice STRING, StockCode STRING, Description STRING, Quantity STRING, InvoiceDate STRING, Price STRING, Customer_id STRING, Country STRING'
);
