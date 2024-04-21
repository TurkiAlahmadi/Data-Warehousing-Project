-- Getting Last Date
SET LAST_PURCHASE_DATE_SK = (SELECT MAX(PURCHASE_DATE_SK) FROM TPCDS.INTERMEDIATE.DAILY_CUSTOMERS);

-- Removing partial records from the last date
DELETE FROM TPCDS.INTERMEDIATE.DAILY_CUSTOMERS WHERE PURCHASE_DATE_SK=$LAST_PURCHASE_DATE_SK;


CREATE OR REPLACE TEMPORARY TABLE TPCDS.INTERMEDIATE.DAILY_CUSTOMERS_TMP AS (
-- compiling all incremental customer counts
with catalog_customers as (
    SELECT 
            CS_BILL_CUSTOMER_SK as CUSTOMER_SK,
            CS_SOLD_DATE_SK as PURCHASE_DATE_SK,
            TRUE as CATALOG_PURCHASE
    from tpcds.raw.catalog_sales
    WHERE purchase_date_sk >= NVL($LAST_PURCHASE_DATE_SK,0) 
        and CS_QUANTITY is not null
        and CS_SALES_PRICE is not null

),
web_customers as (
    SELECT 
            WS_BILL_CUSTOMER_SK as CUSTOMER_SK,
            WS_SOLD_DATE_SK as PURCHASE_DATE_SK,
            TRUE as WEB_PURCHASE
    from tpcds.raw.web_sales
    WHERE purchase_date_sk >= NVL($LAST_PURCHASE_DATE_SK,0) 
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