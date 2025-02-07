CREATE TABLE STV2024101038__DWH.h_accounts (
  h_account_pk BIGINT PRIMARY KEY,
  account_id VARCHAR(255),
  load_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  load_src VARCHAR(50)
)
SEGMENTED BY h_account_pk all nodes;
CREATE TABLE STV2024101038__DWH.h_currency (
  h_currency_pk BIGINT PRIMARY KEY,
  currency_code VARCHAR(3),
  load_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  load_src VARCHAR(50)
)
SEGMENTED BY h_currency_pk all nodes;
CREATE TABLE STV2024101038__DWH.l_transactions (
  operation_id UUID PRIMARY KEY,
  h_account_from_pk BIGINT,
  h_account_to_pk BIGINT,
  h_currency_pk BIGINT,
  load_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  load_src VARCHAR(50),
  FOREIGN KEY (h_account_from_pk) REFERENCES STV2024101038__DWH.h_accounts(h_account_pk),
  FOREIGN KEY (h_account_to_pk) REFERENCES STV2024101038__DWH.h_accounts(h_account_pk),
  FOREIGN KEY (h_currency_pk) REFERENCES STV2024101038__DWH.h_currency(h_currency_pk)
)
SEGMENTED BY operation_id all nodes;
CREATE TABLE STV2024101038__DWH.s_transaction_details (
  hk_transaction_hashdiff BIGINT, 
  operation_id UUID,
  transaction_type VARCHAR(50),
  amount BIGINT,
  country VARCHAR(100),
  status VARCHAR(50),
  transaction_dt TIMESTAMP,
  load_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  load_src VARCHAR(50),
  PRIMARY KEY (hk_transaction_hashdiff),
  FOREIGN KEY (operation_id) REFERENCES STV2024101038__DWH.l_transactions(operation_id)
)
SEGMENTED BY hk_transaction_hashdiff all nodes;
CREATE TABLE STV2024101038__DWH.s_currency_history (
  hk_currency_hashdiff BIGINT, 
  h_currency_pk BIGINT,
  h_currency_with_pk BIGINT,
  currency_code_div DECIMAL(10,6),
  date_update DATE,
  load_dt TIMESTAMP,
  load_src VARCHAR(50),
  PRIMARY KEY (hk_currency_hashdiff),
  FOREIGN KEY (h_currency_pk) REFERENCES STV2024101038__DWH.h_currency(h_currency_pk),
  FOREIGN KEY (h_currency_with_pk) REFERENCES STV2024101038__DWH.h_currency(h_currency_pk)
)
SEGMENTED BY hk_currency_hashdiff all nodes;


CREATE TABLE STV2024101038__STAGING.currencies (
  date_update date, 
  currency_code varchar(3),
  currency_code_with varchar(3),
  currency_with_div DECIMAL(10,6)
)
ORDER BY date_update
SEGMENTED BY HASH(date_update) ALL NODES
PARTITION BY date_update::date;


CREATE TABLE STV2024101038__STAGING.transactions (
  operation_id uuid,
  account_number_from bigint,
  account_number_to bigint,
  currency_code varchar(3),
  country varchar(20),
  status varchar(20),
  transaction_type varchar(30),
  amount bigint,
  transaction_dt TIMESTAMP
)
ORDER BY transaction_dt
SEGMENTED BY HASH(transaction_dt) ALL NODES
PARTITION BY transaction_dt::date;


CREATE OR REPLACE VIEW STV2024101038__DWH.global_metrics AS
  WITH RatesCurrency AS (        
  SELECT c.currency_code, c.currency_code_with, c.currency_with_div
      FROM STV2024101038__STAGING.currencies c
      
      WHERE c.date_update = (SELECT MAX(date_update) FROM STV2024101038__STAGING.currencies) and c.currency_code_with = (SELECT c.currency_code
      FROM STV2024101038__STAGING.currencies c
      LEFT JOIN
          STV2024101038__STAGING.transactions t ON t.currency_code = c.currency_code
      WHERE t.country = 'usa'
      LIMIT 1 )
      
  ),
  TransactionData AS (
      SELECT
              t.operation_id, 
              t.account_number_from,
              t.account_number_to,
              t.currency_code,
              t.amount,
              t.transaction_dt
          FROM
              STV2024101038__STAGING.transactions t
          where t.account_number_from <> -1
          
  )
  SELECT
      td.transaction_dt::date AS date_update,
      td.currency_code AS currency_from,
      SUM(td.amount * COALESCE(rc.currency_with_div, 1)) AS amount_total,  -- Перевод суммы в доллары
      COUNT(td.operation_id) AS cnt_transactions,  -- Общее количество транзакций
      AVG(td.amount) AS avg_transactions_per_account,  -- Средний объем транзакций с аккаунта
      COUNT(DISTINCT td.account_number_from ) AS cnt_accounts_make_transactions  -- Количество уникальных аккаунтов
  FROM
      TransactionData td
      LEFT JOIN
          RatesCurrency rc ON td.currency_code = rc.currency_code
          
  GROUP BY
      td.transaction_dt::date,
      td.currency_code
  ORDER BY
      date_update,
      currency_from;



