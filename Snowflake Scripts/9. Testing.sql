-- Customer Dimension

-- customer_id is not null
SELECT COUNT(*) = 0 FROM TPCDS.ANALYTICS.CUSTOMER_DIM 
WHERE CUSTOMER_SK is null;


-- Weekly Sales Inventory

-- warehouse_sk, item_sk, sold_wk_sk are unique
SELECT COUNT(*) = 0 FROM (
        SELECT warehouse_sk, item_sk, sold_wk_sk
        FROM TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY
        GROUP BY 1,2,3
        HAVING COUNT(*) > 1);

-- relationship
SELECT COUNT(*) = 0 FROM (
        SELECT DIM.I_ITEM_SK
        FROM TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY FACT
        LEFT JOIN TPCDS.RAW.ITEM DIM
        ON DIM.I_ITEM_SK = FACT.ITEM_SK
        WHERE DIM.I_ITEM_SK IS NULL);

-- accepted value
SELECT COUNT(*) = 0 
FROM TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY
WHERE warehouse_sk NOT IN (1,2,3,4,5);

-- Weekly Customer Count

-- purchase_wk_sk is unique
SELECT COUNT(*) = 0 
FROM TPCDS.ANALYTICS.WEEKLY_CUSTOMER_COUNT
WHERE purchase_wk_sk is null;

-- relationship
SELECT COUNT(*) = 0 FROM (
        SELECT DIM.D_DATE_SK
        FROM TPCDS.ANALYTICS.WEEKLY_CUSTOMER_COUNT FACT
        LEFT JOIN TPCDS.RAW.DATE_DIM DIM
        ON DIM.D_DATE_SK = FACT.PURCHASE_WK_SK
        WHERE DIM.D_DATE_SK IS NULL);