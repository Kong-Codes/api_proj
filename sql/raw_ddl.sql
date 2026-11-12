create database if not exists PAYBRIDGE;
create schema if not exists PAYBRIDGE.RAW;

-- Stored as received: everything is text, typing happens in dbt staging
create table if not exists PAYBRIDGE.RAW.PARTNER_TRANSACTIONS (
    TRANSACTION_ID  varchar,
    PARTNER_ID      varchar,
    MERCHANT_ID     varchar,
    AMOUNT          varchar,
    CURRENCY        varchar,
    STATUS          varchar,
    TRANSACTION_TS  varchar,
    _LOADED_AT      timestamp_ntz default current_timestamp(),
    _BATCH_ID       varchar
);

create table if not exists PAYBRIDGE.RAW.PARTNER_DAILY_REPORTS (
    PARTNER_ID      varchar,
    REPORT_DATE     date,
    TOTAL_AMOUNT    number(18,2),
    _LOADED_AT      timestamp_ntz default current_timestamp()
);

create table if not exists PAYBRIDGE.RAW.INGESTION_LOG (
    BATCH_ID        varchar,
    PARTNER_ID      varchar,
    LOAD_DATE       date,
    ROWS_FETCHED    number,
    ROWS_EXPECTED   number,
    STATUS          varchar,
    ERROR_MESSAGE   varchar,
    RUN_TS          timestamp_ntz default current_timestamp()
);
