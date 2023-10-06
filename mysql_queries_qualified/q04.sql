-- Copyright (c) 2022, Oracle and/or its affiliates.

-- Licensed under the Apache License, Version 2.0 (the "License");

--  you may not use this file except in compliance with the License.

--  You may obtain a copy of the License at

--

--     https://www.apache.org/licenses/LICENSE-2.0

--

--  Unless required by applicable law or agreed to in writing, software

--  distributed under the License is distributed on an "AS IS" BASIS,

--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

--  See the License for the specific language governing permissions and

--  limitations under the License.

-- Copyright (c) 2022, Transaction Processing Performance Council

WITH YEAR_TOTAL AS (
        SELECT
            C_CUSTOMER_ID CUSTOMER_ID,
            C_FIRST_NAME CUSTOMER_FIRST_NAME,
            C_LAST_NAME CUSTOMER_LAST_NAME,
            C_PREFERRED_CUST_FLAG CUSTOMER_PREFERRED_CUST_FLAG,
            C_BIRTH_COUNTRY CUSTOMER_BIRTH_COUNTRY,
            C_LOGIN CUSTOMER_LOGIN,
            C_EMAIL_ADDRESS CUSTOMER_EMAIL_ADDRESS,
            D_YEAR DYEAR,
            SUM( ( (
                        SS_EXT_LIST_PRICE - SS_EXT_WHOLESALE_COST - SS_EXT_DISCOUNT_AMT
                    ) + SS_EXT_SALES_PRICE
                ) / 2
            ) YEAR_TOTAL,
            's' SALE_TYPE
        FROM
            CUSTOMER,
            STORE_SALES,
            DATE_DIM
        WHERE
            C_CUSTOMER_SK = SS_CUSTOMER_SK
            AND SS_SOLD_DATE_SK = D_DATE_SK
        GROUP BY
            C_CUSTOMER_ID,
            C_FIRST_NAME,
            C_LAST_NAME,
            C_PREFERRED_CUST_FLAG,
            C_BIRTH_COUNTRY,
            C_LOGIN,
            C_EMAIL_ADDRESS,
            D_YEAR
        UNION ALL
        SELECT
            C_CUSTOMER_ID CUSTOMER_ID,
            C_FIRST_NAME CUSTOMER_FIRST_NAME,
            C_LAST_NAME CUSTOMER_LAST_NAME,
            C_PREFERRED_CUST_FLAG CUSTOMER_PREFERRED_CUST_FLAG,
            C_BIRTH_COUNTRY CUSTOMER_BIRTH_COUNTRY,
            C_LOGIN CUSTOMER_LOGIN,
            C_EMAIL_ADDRESS CUSTOMER_EMAIL_ADDRESS,
            D_YEAR DYEAR,
            SUM( ( ( (
                            CS_EXT_LIST_PRICE - CS_EXT_WHOLESALE_COST - CS_EXT_DISCOUNT_AMT
                        ) + CS_EXT_SALES_PRICE
                    ) / 2
                )
            ) YEAR_TOTAL,
            'c' SALE_TYPE
        FROM
            CUSTOMER,
            CATALOG_SALES,
            DATE_DIM
        WHERE
            C_CUSTOMER_SK = CS_BILL_CUSTOMER_SK
            AND CS_SOLD_DATE_SK = D_DATE_SK
        GROUP BY
            C_CUSTOMER_ID,
            C_FIRST_NAME,
            C_LAST_NAME,
            C_PREFERRED_CUST_FLAG,
            C_BIRTH_COUNTRY,
            C_LOGIN,
            C_EMAIL_ADDRESS,
            D_YEAR
        UNION ALL
        SELECT
            C_CUSTOMER_ID CUSTOMER_ID,
            C_FIRST_NAME CUSTOMER_FIRST_NAME,
            C_LAST_NAME CUSTOMER_LAST_NAME,
            C_PREFERRED_CUST_FLAG CUSTOMER_PREFERRED_CUST_FLAG,
            C_BIRTH_COUNTRY CUSTOMER_BIRTH_COUNTRY,
            C_LOGIN CUSTOMER_LOGIN,
            C_EMAIL_ADDRESS CUSTOMER_EMAIL_ADDRESS,
            D_YEAR DYEAR,
            SUM( ( ( (
                            WS_EXT_LIST_PRICE - WS_EXT_WHOLESALE_COST - WS_EXT_DISCOUNT_AMT
                        ) + WS_EXT_SALES_PRICE
                    ) / 2
                )
            ) YEAR_TOTAL,
            'w' SALE_TYPE
        FROM
            CUSTOMER,
            WEB_SALES,
            DATE_DIM
        WHERE
            C_CUSTOMER_SK = WS_BILL_CUSTOMER_SK
            AND WS_SOLD_DATE_SK = D_DATE_SK
        GROUP BY
            C_CUSTOMER_ID,
            C_FIRST_NAME,
            C_LAST_NAME,
            C_PREFERRED_CUST_FLAG,
            C_BIRTH_COUNTRY,
            C_LOGIN,
            C_EMAIL_ADDRESS,
            D_YEAR
    )
SELECT
    T_S_SECYEAR.CUSTOMER_ID,
    T_S_SECYEAR.CUSTOMER_FIRST_NAME,
    T_S_SECYEAR.CUSTOMER_LAST_NAME,
    T_S_SECYEAR.CUSTOMER_BIRTH_COUNTRY
FROM
    YEAR_TOTAL T_S_FIRSTYEAR,
    YEAR_TOTAL T_S_SECYEAR,
    YEAR_TOTAL T_C_FIRSTYEAR,
    YEAR_TOTAL T_C_SECYEAR,
    YEAR_TOTAL T_W_FIRSTYEAR,
    YEAR_TOTAL T_W_SECYEAR
WHERE
    T_S_SECYEAR.CUSTOMER_ID = T_S_FIRSTYEAR.CUSTOMER_ID
    AND T_S_FIRSTYEAR.CUSTOMER_ID = T_C_SECYEAR.CUSTOMER_ID
    AND T_S_FIRSTYEAR.CUSTOMER_ID = T_C_FIRSTYEAR.CUSTOMER_ID
    AND T_S_FIRSTYEAR.CUSTOMER_ID = T_W_FIRSTYEAR.CUSTOMER_ID
    AND T_S_FIRSTYEAR.CUSTOMER_ID = T_W_SECYEAR.CUSTOMER_ID
    AND T_S_FIRSTYEAR.SALE_TYPE = 's'
    AND T_C_FIRSTYEAR.SALE_TYPE = 'c'
    AND T_W_FIRSTYEAR.SALE_TYPE = 'w'
    AND T_S_SECYEAR.SALE_TYPE = 's'
    AND T_C_SECYEAR.SALE_TYPE = 'c'
    AND T_W_SECYEAR.SALE_TYPE = 'w'
    AND T_S_FIRSTYEAR.DYEAR = 1999
    AND T_S_SECYEAR.DYEAR = 1999 + 1
    AND T_C_FIRSTYEAR.DYEAR = 1999
    AND T_C_SECYEAR.DYEAR = 1999 + 1
    AND T_W_FIRSTYEAR.DYEAR = 1999
    AND T_W_SECYEAR.DYEAR = 1999 + 1
    AND T_S_FIRSTYEAR.YEAR_TOTAL > 0
    AND T_C_FIRSTYEAR.YEAR_TOTAL > 0
    AND T_W_FIRSTYEAR.YEAR_TOTAL > 0
    AND CASE
        WHEN T_C_FIRSTYEAR.YEAR_TOTAL > 0 THEN T_C_SECYEAR.YEAR_TOTAL / T_C_FIRSTYEAR.YEAR_TOTAL
        ELSE NULL
    END > CASE
        WHEN T_S_FIRSTYEAR.YEAR_TOTAL > 0 THEN T_S_SECYEAR.YEAR_TOTAL / T_S_FIRSTYEAR.YEAR_TOTAL
        ELSE NULL
    END
    AND CASE
        WHEN T_C_FIRSTYEAR.YEAR_TOTAL > 0 THEN T_C_SECYEAR.YEAR_TOTAL / T_C_FIRSTYEAR.YEAR_TOTAL
        ELSE NULL
    END > CASE
        WHEN T_W_FIRSTYEAR.YEAR_TOTAL > 0 THEN T_W_SECYEAR.YEAR_TOTAL / T_W_FIRSTYEAR.YEAR_TOTAL
        ELSE NULL
    END
ORDER BY
    T_S_SECYEAR.CUSTOMER_ID,
    T_S_SECYEAR.CUSTOMER_FIRST_NAME,
    T_S_SECYEAR.CUSTOMER_LAST_NAME,
    T_S_SECYEAR.CUSTOMER_BIRTH_COUNTRY
LIMIT 100;