SELECT
    C_LAST_NAME,
    C_FIRST_NAME,
    CA_CITY,
    BOUGHT_CITY,
    SS_TICKET_NUMBER,
    AMT,
    PROFIT
FROM
    CUSTOMER_ADDRESS CURRENT_ADDR
    JOIN CUSTOMER
    JOIN (
        SELECT
            SS_TICKET_NUMBER,
            SS_CUSTOMER_SK,
            CA_CITY BOUGHT_CITY,
            SUM(SS_COUPON_AMT) AMT,
            SUM(SS_NET_PROFIT) PROFIT
        FROM STORE
            JOIN STORE_SALES
            JOIN HOUSEHOLD_DEMOGRAPHICS
            JOIN DATE_DIM
            JOIN CUSTOMER_ADDRESS
        WHERE
            STORE_SALES.SS_SOLD_DATE_SK = DATE_DIM.D_DATE_SK
            AND STORE_SALES.SS_STORE_SK = STORE.S_STORE_SK
            AND STORE_SALES.SS_HDEMO_SK = HOUSEHOLD_DEMOGRAPHICS.HD_DEMO_SK
            AND STORE_SALES.SS_ADDR_SK = CUSTOMER_ADDRESS.CA_ADDRESS_SK
            AND (
                HOUSEHOLD_DEMOGRAPHICS.HD_DEP_COUNT = 0
                OR HOUSEHOLD_DEMOGRAPHICS.HD_VEHICLE_COUNT = 1
            )
            AND DATE_DIM.D_DOW IN (6, 0)
            AND DATE_DIM.D_YEAR IN (2000, 2000 + 1, 2000 + 2)
            AND STORE.S_CITY IN (
                'Five Forks',
                'Oakland',
                'Fairview',
                'Winchester',
                'Farmington'
            )
        GROUP BY
            SS_TICKET_NUMBER,
            SS_CUSTOMER_SK,
            SS_ADDR_SK,
            CA_CITY
    ) DN
WHERE
    SS_CUSTOMER_SK = C_CUSTOMER_SK
    AND CUSTOMER.C_CURRENT_ADDR_SK = CURRENT_ADDR.CA_ADDRESS_SK
    AND CURRENT_ADDR.CA_CITY <> BOUGHT_CITY
ORDER BY
    C_LAST_NAME,
    C_FIRST_NAME,
    CA_CITY,
    BOUGHT_CITY,
    SS_TICKET_NUMBER
LIMIT 100;