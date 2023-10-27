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

SELECT
    W_WAREHOUSE_NAME,
    W_WAREHOUSE_SQ_FT,
    W_CITY,
    W_COUNTY,
    W_STATE,
    W_COUNTRY,
    SHIP_CARRIERS,
    YEAR,
    SUM(JAN_SALES) AS JAN_SALES,
    SUM(FEB_SALES) AS FEB_SALES,
    SUM(MAR_SALES) AS MAR_SALES,
    SUM(APR_SALES) AS APR_SALES,
    SUM(MAY_SALES) AS MAY_SALES,
    SUM(JUN_SALES) AS JUN_SALES,
    SUM(JUL_SALES) AS JUL_SALES,
    SUM(AUG_SALES) AS AUG_SALES,
    SUM(SEP_SALES) AS SEP_SALES,
    SUM(OCT_SALES) AS OCT_SALES,
    SUM(NOV_SALES) AS NOV_SALES,
    SUM(DEC_SALES) AS DEC_SALES,
    SUM(JAN_SALES / W_WAREHOUSE_SQ_FT) AS JAN_SALES_PER_SQ_FOOT,
    SUM(FEB_SALES / W_WAREHOUSE_SQ_FT) AS FEB_SALES_PER_SQ_FOOT,
    SUM(MAR_SALES / W_WAREHOUSE_SQ_FT) AS MAR_SALES_PER_SQ_FOOT,
    SUM(APR_SALES / W_WAREHOUSE_SQ_FT) AS APR_SALES_PER_SQ_FOOT,
    SUM(MAY_SALES / W_WAREHOUSE_SQ_FT) AS MAY_SALES_PER_SQ_FOOT,
    SUM(JUN_SALES / W_WAREHOUSE_SQ_FT) AS JUN_SALES_PER_SQ_FOOT,
    SUM(JUL_SALES / W_WAREHOUSE_SQ_FT) AS JUL_SALES_PER_SQ_FOOT,
    SUM(AUG_SALES / W_WAREHOUSE_SQ_FT) AS AUG_SALES_PER_SQ_FOOT,
    SUM(SEP_SALES / W_WAREHOUSE_SQ_FT) AS SEP_SALES_PER_SQ_FOOT,
    SUM(OCT_SALES / W_WAREHOUSE_SQ_FT) AS OCT_SALES_PER_SQ_FOOT,
    SUM(NOV_SALES / W_WAREHOUSE_SQ_FT) AS NOV_SALES_PER_SQ_FOOT,
    SUM(DEC_SALES / W_WAREHOUSE_SQ_FT) AS DEC_SALES_PER_SQ_FOOT,
    SUM(JAN_NET) AS JAN_NET,
    SUM(FEB_NET) AS FEB_NET,
    SUM(MAR_NET) AS MAR_NET,
    SUM(APR_NET) AS APR_NET,
    SUM(MAY_NET) AS MAY_NET,
    SUM(JUN_NET) AS JUN_NET,
    SUM(JUL_NET) AS JUL_NET,
    SUM(AUG_NET) AS AUG_NET,
    SUM(SEP_NET) AS SEP_NET,
    SUM(OCT_NET) AS OCT_NET,
    SUM(NOV_NET) AS NOV_NET,
    SUM(DEC_NET) AS DEC_NET
FROM (
        SELECT
            W_WAREHOUSE_NAME,
            W_WAREHOUSE_SQ_FT,
            W_CITY,
            W_COUNTY,
            W_STATE,
            W_COUNTRY,
            'USPS' || ',' || 'TBS' AS SHIP_CARRIERS,
            D_YEAR AS YEAR,
            SUM(
                CASE
                    WHEN D_MOY = 1 THEN WS_SALES_PRICE * WS_QUANTITY
                    ELSE 0
                END
            ) AS JAN_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 2 THEN WS_SALES_PRICE * WS_QUANTITY
                    ELSE 0
                END
            ) AS FEB_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 3 THEN WS_SALES_PRICE * WS_QUANTITY
                    ELSE 0
                END
            ) AS MAR_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 4 THEN WS_SALES_PRICE * WS_QUANTITY
                    ELSE 0
                END
            ) AS APR_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 5 THEN WS_SALES_PRICE * WS_QUANTITY
                    ELSE 0
                END
            ) AS MAY_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 6 THEN WS_SALES_PRICE * WS_QUANTITY
                    ELSE 0
                END
            ) AS JUN_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 7 THEN WS_SALES_PRICE * WS_QUANTITY
                    ELSE 0
                END
            ) AS JUL_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 8 THEN WS_SALES_PRICE * WS_QUANTITY
                    ELSE 0
                END
            ) AS AUG_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 9 THEN WS_SALES_PRICE * WS_QUANTITY
                    ELSE 0
                END
            ) AS SEP_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 10 THEN WS_SALES_PRICE * WS_QUANTITY
                    ELSE 0
                END
            ) AS OCT_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 11 THEN WS_SALES_PRICE * WS_QUANTITY
                    ELSE 0
                END
            ) AS NOV_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 12 THEN WS_SALES_PRICE * WS_QUANTITY
                    ELSE 0
                END
            ) AS DEC_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 1 THEN WS_NET_PAID_INC_SHIP_TAX * WS_QUANTITY
                    ELSE 0
                END
            ) AS JAN_NET,
            SUM(
                CASE
                    WHEN D_MOY = 2 THEN WS_NET_PAID_INC_SHIP_TAX * WS_QUANTITY
                    ELSE 0
                END
            ) AS FEB_NET,
            SUM(
                CASE
                    WHEN D_MOY = 3 THEN WS_NET_PAID_INC_SHIP_TAX * WS_QUANTITY
                    ELSE 0
                END
            ) AS MAR_NET,
            SUM(
                CASE
                    WHEN D_MOY = 4 THEN WS_NET_PAID_INC_SHIP_TAX * WS_QUANTITY
                    ELSE 0
                END
            ) AS APR_NET,
            SUM(
                CASE
                    WHEN D_MOY = 5 THEN WS_NET_PAID_INC_SHIP_TAX * WS_QUANTITY
                    ELSE 0
                END
            ) AS MAY_NET,
            SUM(
                CASE
                    WHEN D_MOY = 6 THEN WS_NET_PAID_INC_SHIP_TAX * WS_QUANTITY
                    ELSE 0
                END
            ) AS JUN_NET,
            SUM(
                CASE
                    WHEN D_MOY = 7 THEN WS_NET_PAID_INC_SHIP_TAX * WS_QUANTITY
                    ELSE 0
                END
            ) AS JUL_NET,
            SUM(
                CASE
                    WHEN D_MOY = 8 THEN WS_NET_PAID_INC_SHIP_TAX * WS_QUANTITY
                    ELSE 0
                END
            ) AS AUG_NET,
            SUM(
                CASE
                    WHEN D_MOY = 9 THEN WS_NET_PAID_INC_SHIP_TAX * WS_QUANTITY
                    ELSE 0
                END
            ) AS SEP_NET,
            SUM(
                CASE
                    WHEN D_MOY = 10 THEN WS_NET_PAID_INC_SHIP_TAX * WS_QUANTITY
                    ELSE 0
                END
            ) AS OCT_NET,
            SUM(
                CASE
                    WHEN D_MOY = 11 THEN WS_NET_PAID_INC_SHIP_TAX * WS_QUANTITY
                    ELSE 0
                END
            ) AS NOV_NET,
            SUM(
                CASE
                    WHEN D_MOY = 12 THEN WS_NET_PAID_INC_SHIP_TAX * WS_QUANTITY
                    ELSE 0
                END
            ) AS DEC_NET
        FROM
            WEB_SALES,
            WAREHOUSE,
            DATE_DIM,
            TIME_DIM,
            SHIP_MODE
        WHERE
            WS_WAREHOUSE_SK = W_WAREHOUSE_SK
            AND WS_SOLD_DATE_SK = D_DATE_SK
            AND WS_SOLD_TIME_SK = T_TIME_SK
            AND WS_SHIP_MODE_SK = SM_SHIP_MODE_SK
            AND D_YEAR = 2001
            AND T_TIME BETWEEN 9453 AND 9453 + 28800
            AND SM_CARRIER IN ('MSC', 'GERMA')
        GROUP BY
            W_WAREHOUSE_NAME,
            W_WAREHOUSE_SQ_FT,
            W_CITY,
            W_COUNTY,
            W_STATE,
            W_COUNTRY,
            D_YEAR
        UNION ALL
        SELECT
            W_WAREHOUSE_NAME,
            W_WAREHOUSE_SQ_FT,
            W_CITY,
            W_COUNTY,
            W_STATE,
            W_COUNTRY,
            'MSC' || ',' || 'GERMA' AS SHIP_CARRIERS,
            D_YEAR AS YEAR,
            SUM(
                CASE
                    WHEN D_MOY = 1 THEN CS_EXT_LIST_PRICE * CS_QUANTITY
                    ELSE 0
                END
            ) AS JAN_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 2 THEN CS_EXT_LIST_PRICE * CS_QUANTITY
                    ELSE 0
                END
            ) AS FEB_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 3 THEN CS_EXT_LIST_PRICE * CS_QUANTITY
                    ELSE 0
                END
            ) AS MAR_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 4 THEN CS_EXT_LIST_PRICE * CS_QUANTITY
                    ELSE 0
                END
            ) AS APR_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 5 THEN CS_EXT_LIST_PRICE * CS_QUANTITY
                    ELSE 0
                END
            ) AS MAY_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 6 THEN CS_EXT_LIST_PRICE * CS_QUANTITY
                    ELSE 0
                END
            ) AS JUN_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 7 THEN CS_EXT_LIST_PRICE * CS_QUANTITY
                    ELSE 0
                END
            ) AS JUL_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 8 THEN CS_EXT_LIST_PRICE * CS_QUANTITY
                    ELSE 0
                END
            ) AS AUG_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 9 THEN CS_EXT_LIST_PRICE * CS_QUANTITY
                    ELSE 0
                END
            ) AS SEP_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 10 THEN CS_EXT_LIST_PRICE * CS_QUANTITY
                    ELSE 0
                END
            ) AS OCT_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 11 THEN CS_EXT_LIST_PRICE * CS_QUANTITY
                    ELSE 0
                END
            ) AS NOV_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 12 THEN CS_EXT_LIST_PRICE * CS_QUANTITY
                    ELSE 0
                END
            ) AS DEC_SALES,
            SUM(
                CASE
                    WHEN D_MOY = 1 THEN CS_NET_PAID_INC_SHIP * CS_QUANTITY
                    ELSE 0
                END
            ) AS JAN_NET,
            SUM(
                CASE
                    WHEN D_MOY = 2 THEN CS_NET_PAID_INC_SHIP * CS_QUANTITY
                    ELSE 0
                END
            ) AS FEB_NET,
            SUM(
                CASE
                    WHEN D_MOY = 3 THEN CS_NET_PAID_INC_SHIP * CS_QUANTITY
                    ELSE 0
                END
            ) AS MAR_NET,
            SUM(
                CASE
                    WHEN D_MOY = 4 THEN CS_NET_PAID_INC_SHIP * CS_QUANTITY
                    ELSE 0
                END
            ) AS APR_NET,
            SUM(
                CASE
                    WHEN D_MOY = 5 THEN CS_NET_PAID_INC_SHIP * CS_QUANTITY
                    ELSE 0
                END
            ) AS MAY_NET,
            SUM(
                CASE
                    WHEN D_MOY = 6 THEN CS_NET_PAID_INC_SHIP * CS_QUANTITY
                    ELSE 0
                END
            ) AS JUN_NET,
            SUM(
                CASE
                    WHEN D_MOY = 7 THEN CS_NET_PAID_INC_SHIP * CS_QUANTITY
                    ELSE 0
                END
            ) AS JUL_NET,
            SUM(
                CASE
                    WHEN D_MOY = 8 THEN CS_NET_PAID_INC_SHIP * CS_QUANTITY
                    ELSE 0
                END
            ) AS AUG_NET,
            SUM(
                CASE
                    WHEN D_MOY = 9 THEN CS_NET_PAID_INC_SHIP * CS_QUANTITY
                    ELSE 0
                END
            ) AS SEP_NET,
            SUM(
                CASE
                    WHEN D_MOY = 10 THEN CS_NET_PAID_INC_SHIP * CS_QUANTITY
                    ELSE 0
                END
            ) AS OCT_NET,
            SUM(
                CASE
                    WHEN D_MOY = 11 THEN CS_NET_PAID_INC_SHIP * CS_QUANTITY
                    ELSE 0
                END
            ) AS NOV_NET,
            SUM(
                CASE
                    WHEN D_MOY = 12 THEN CS_NET_PAID_INC_SHIP * CS_QUANTITY
                    ELSE 0
                END
            ) AS DEC_NET
        FROM
            CATALOG_SALES,
            WAREHOUSE,
            DATE_DIM,
            TIME_DIM,
            SHIP_MODE
        WHERE
            CS_WAREHOUSE_SK = W_WAREHOUSE_SK
            AND CS_SOLD_DATE_SK = D_DATE_SK
            AND CS_SOLD_TIME_SK = T_TIME_SK
            AND CS_SHIP_MODE_SK = SM_SHIP_MODE_SK
            AND D_YEAR = 2001
            AND T_TIME BETWEEN 9453 AND 9453 + 28800
            AND SM_CARRIER IN ('MSC', 'GERMA')
        GROUP BY
            W_WAREHOUSE_NAME,
            W_WAREHOUSE_SQ_FT,
            W_CITY,
            W_COUNTY,
            W_STATE,
            W_COUNTRY,
            D_YEAR
    ) X
GROUP BY
    W_WAREHOUSE_NAME,
    W_WAREHOUSE_SQ_FT,
    W_CITY,
    W_COUNTY,
    W_STATE,
    W_COUNTRY,
    SHIP_CARRIERS,
    YEAR
ORDER BY W_WAREHOUSE_NAME
LIMIT 100;