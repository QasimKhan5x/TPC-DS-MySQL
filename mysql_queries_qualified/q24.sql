WITH SSALES AS (
        SELECT
            C_LAST_NAME,
            C_FIRST_NAME,
            S_STORE_NAME,
            CA_STATE,
            S_STATE,
            I_COLOR,
            I_CURRENT_PRICE,
            I_MANAGER_ID,
            I_UNITS,
            I_SIZE,
            SUM(SS_SALES_PRICE) NETPAID
        FROM STORE
            JOIN CUSTOMER_ADDRESS
            JOIN CUSTOMER
            JOIN STORE_SALES
            JOIN ITEM
            JOIN STORE_RETURNS
        WHERE
            SS_TICKET_NUMBER = SR_TICKET_NUMBER
            AND SS_ITEM_SK = SR_ITEM_SK
            AND SS_CUSTOMER_SK = C_CUSTOMER_SK
            AND SS_ITEM_SK = I_ITEM_SK
            AND C_CURRENT_ADDR_SK = CA_ADDRESS_SK
            AND C_BIRTH_COUNTRY = UPPER(CA_COUNTRY)
            AND S_ZIP = CA_ZIP
            AND S_MARKET_ID = 10
        GROUP BY
            C_LAST_NAME,
            C_FIRST_NAME,
            S_STORE_NAME,
            CA_STATE,
            S_STATE,
            I_COLOR,
            I_CURRENT_PRICE,
            I_MANAGER_ID,
            I_UNITS,
            I_SIZE
    )
SELECT
    C_LAST_NAME,
    C_FIRST_NAME,
    S_STORE_NAME,
    SUM(NETPAID) PAID
FROM SSALES
WHERE I_COLOR = 'snow'
GROUP BY
    C_LAST_NAME,
    C_FIRST_NAME,
    S_STORE_NAME
HAVING SUM(NETPAID) > (
        SELECT 0.05 * AVG(NETPAID)
        FROM SSALES
    )
ORDER BY
    C_LAST_NAME,
    C_FIRST_NAME,
    S_STORE_NAME;

WITH SSALES AS (
        SELECT
            C_LAST_NAME,
            C_FIRST_NAME,
            S_STORE_NAME,
            CA_STATE,
            S_STATE,
            I_COLOR,
            I_CURRENT_PRICE,
            I_MANAGER_ID,
            I_UNITS,
            I_SIZE,
            SUM(SS_NET_PAID) NETPAID
        FROM STORE
            JOIN STORE_SALES
            JOIN ITEM
            JOIN STORE_RETURNS
            JOIN CUSTOMER
            JOIN CUSTOMER_ADDRESS
        WHERE
            SS_TICKET_NUMBER = SR_TICKET_NUMBER
            AND SS_ITEM_SK = SR_ITEM_SK
            AND SS_CUSTOMER_SK = C_CUSTOMER_SK
            AND SS_ITEM_SK = I_ITEM_SK
            AND SS_STORE_SK = S_STORE_SK
            AND C_BIRTH_COUNTRY = UPPER(CA_COUNTRY)
            AND S_ZIP = CA_ZIP
            AND S_MARKET_ID = 6
        GROUP BY
            C_LAST_NAME,
            C_FIRST_NAME,
            S_STORE_NAME,
            CA_STATE,
            S_STATE,
            I_COLOR,
            I_CURRENT_PRICE,
            I_MANAGER_ID,
            I_UNITS,
            I_SIZE
    )
SELECT
    C_LAST_NAME,
    C_FIRST_NAME,
    S_STORE_NAME,
    SUM(NETPAID) PAID
FROM SSALES
WHERE I_COLOR = 'burlywood'
GROUP BY
    C_LAST_NAME,
    C_FIRST_NAME,
    S_STORE_NAME
HAVING SUM(NETPAID) > (
        SELECT 0.05 * AVG(NETPAID)
        FROM SSALES
    )
ORDER BY
    C_LAST_NAME,
    C_FIRST_NAME,
    S_STORE_NAME;