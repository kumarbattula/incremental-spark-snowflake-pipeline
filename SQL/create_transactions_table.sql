CREATE TABLE IF NOT EXISTS transactions (
    txn_id STRING,
    customer_id STRING,
    amount NUMBER(10,2),
    txn_date DATE,
    load_ts TIMESTAMP
);
