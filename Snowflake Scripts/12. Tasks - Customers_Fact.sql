
--------------------------------------------------------
-- Daily Customer Counts
--------------------------------------------------------
CREATE OR REPLACE PROCEDURE tpcds.intermediate.populating_daily_customer_counts()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
      DECLARE 
        LAST_PURCHASE_DATE_SK number;
    BEGIN
        SELECT MAX(PURCHASE_DATE_SK) INTO :LAST_PURCHASE_DATE_SK FROM TPCDS.INTERMEDIATE.DAILY_CUSTOMERS;
        DELETE FROM TPCDS.INTERMEDIATE.DAILY_CUSTOMERS WHERE PURCHASE_DATE_SK=:LAST_PURCHASE_DATE_SK;
        CREATE OR REPLACE TEMPORARY TABLE TPCDS.INTERMEDIATE.DAILY_CUSTOMERS_TMP AS (
        -- compiling all incremental customer counts
        with catalog_customers as (
            SELECT 
                    CS_BILL_CUSTOMER_SK as CUSTOMER_SK,
                    CS_SOLD_DATE_SK as PURCHASE_DATE_SK,
                    TRUE as CATALOG_PURCHASE
            from tpcds.raw.catalog_sales
            WHERE purchase_date_sk >= NVL(:LAST_PURCHASE_DATE_SK,0) 
                and CS_QUANTITY is not null
                and CS_SALES_PRICE is not null
        
        ),
        web_customers as (
            SELECT 
                    WS_BILL_CUSTOMER_SK as CUSTOMER_SK,
                    WS_SOLD_DATE_SK as PURCHASE_DATE_SK,
                    TRUE as WEB_PURCHASE
            from tpcds.raw.web_sales
            WHERE purchase_date_sk >= NVL(:LAST_PURCHASE_DATE_SK,0) 
                and WS_QUANTITY is not null
                and WS_SALES_PRICE is not null
        ),
        all_customers as (
            SELECT 
                    CUSTOMER_SK,
                    PURCHASE_DATE_SK,
                    CATALOG_PURCHASE,
                    WEB_PURCHASE
            FROM catalog_customers
            FULL OUTER JOIN web_customers USING (CUSTOMER_SK, PURCHASE_DATE_SK)
            ORDER BY 1,2
        ),
        adding_week_number_and_yr_number as (
            SELECT 
                    *,
                    date.wk_num as purchase_wk_num,
                    date.yr_num as purchase_yr_num
            FROM all_customers 
            LEFT JOIN tpcds.raw.date_dim date ON purchase_date_sk = d_date_sk
        )
        
        SELECT 
            	CUSTOMER_SK,
                PURCHASE_DATE_SK,
                PURCHASE_WK_NUM,
                PURCHASE_YR_NUM,
                CATALOG_PURCHASE,
                WEB_PURCHASE 
        FROM adding_week_number_and_yr_number
        ORDER BY 1,2
        );
        
        -- Inserting new records
        INSERT INTO TPCDS.INTERMEDIATE.DAILY_CUSTOMERS
        (	
            CUSTOMER_SK, 
            PURCHASE_DATE_SK, 
            PURCHASE_WK_NUM,
            PURCHASE_YR_NUM,
            CATALOG_PURCHASE,
            WEB_PURCHASE 
        )
        SELECT 
            DISTINCT
        	CUSTOMER_SK,
            PURCHASE_DATE_SK,
            PURCHASE_WK_NUM,
            PURCHASE_YR_NUM,
            CATALOG_PURCHASE,
            WEB_PURCHASE
        FROM TPCDS.INTERMEDIATE.DAILY_CUSTOMERS_TMP;
  END
  $$;

CREATE OR REPLACE TASK tpcds.intermediate.creating_daily_customer_counts
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON * 8 * * * UTC'
    AS
CALL TPCDS.INTERMEDIATE.POPULATING_DAILY_CUSTOMER_COUNTS();

ALTER TASK tpcds.intermediate.creating_daily_customer_counts RESUME;
EXECUTE TASK tpcds.intermediate.creating_daily_customer_counts;

--------------------------------------------------------
-- Weekly Customer Counts
--------------------------------------------------------
CREATE OR REPLACE PROCEDURE tpcds.analytics.populating_weekly_customer_counts()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
      DECLARE 
        LAST_PURCHASE_WK_SK number;
    BEGIN
        SELECT MAX(PURCHASE_WK_SK) INTO :LAST_PURCHASE_WK_SK FROM TPCDS.ANALYTICS.WEEKLY_CUSTOMER_COUNT; 
        DELETE FROM TPCDS.ANALYTICS.WEEKLY_CUSTOMER_COUNT WHERE PURCHASE_WK_SK=:LAST_PURCHASE_WK_SK;
        
        -- compiling all incremental sales records
        CREATE OR REPLACE TEMPORARY TABLE TPCDS.ANALYTICS.WEEKLY_CUSTOMER_COUNT_TMP AS (
        with aggregating_daily_customers_by_week as (
        SELECT 
            MIN(PURCHASE_DATE_SK) AS PURCHASE_WK_SK, 
            PURCHASE_WK_NUM, 
            PURCHASE_YR_NUM, 
            COUNT(DISTINCT (CASE WHEN CATALOG_PURCHASE = TRUE THEN CUSTOMER_SK END)) AS CATALOG_CUSTOMERS_WK, 
            COUNT(DISTINCT (CASE WHEN WEB_PURCHASE = TRUE THEN CUSTOMER_SK END)) AS WEB_CUSTOMERS_WK
        FROM
            TPCDS.INTERMEDIATE.DAILY_CUSTOMERS
        GROUP BY
            2,3
        HAVING 
            PURCHASE_WK_SK >= NVL(:LAST_PURCHASE_WK_SK,0)
        )
        
        SELECT 
            date.d_date_sk AS PURCHASE_WK_SK, 
            PURCHASE_WK_NUM, 
            PURCHASE_YR_NUM,
            CATALOG_CUSTOMERS_WK,
            WEB_CUSTOMERS_WK 
        FROM
            aggregating_daily_customers_by_week daily_customers
        INNER JOIN TPCDS.RAW.DATE_DIM as date
        on daily_customers.PURCHASE_WK_NUM=date.wk_num
        and daily_customers.PURCHASE_YR_NUM=date.yr_num
        and date.day_of_wk_num=0
        );
        
        -- Inserting new records
        INSERT INTO TPCDS.ANALYTICS.WEEKLY_CUSTOMER_COUNT
        (	
        	PURCHASE_WK_SK, 
            PURCHASE_WK_NUM, 
            PURCHASE_YR_NUM,
            CATALOG_CUSTOMERS_WK,
            WEB_CUSTOMERS_WK 
        )
        SELECT 
            PURCHASE_WK_SK, 
            PURCHASE_WK_NUM, 
            PURCHASE_YR_NUM,
            CATALOG_CUSTOMERS_WK,
            WEB_CUSTOMERS_WK 
        FROM TPCDS.ANALYTICS.WEEKLY_CUSTOMER_COUNT_TMP;
  END
  $$;

CREATE OR REPLACE TASK tpcds.analytics.creating_weekly_customer_counts
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON * 8 * * * UTC'
    AS
CALL TPCDS.ANALYTICS.POPULATING_WEEKLY_CUSTOMER_COUNTS();

ALTER TASK tpcds.analytics.creating_weekly_customer_counts RESUME;
EXECUTE TASK tpcds.analytics.creating_weekly_customer_counts;
