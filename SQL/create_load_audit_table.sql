CREATE OR REPLACE TABLE load_audit (
    file_name STRING,
    load_date DATE,
    record_count INT,
    status STRING,
    load_ts TIMESTAMP
);