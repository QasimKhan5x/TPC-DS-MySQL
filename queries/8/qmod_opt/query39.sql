WITH INV AS (
    SELECT 
        W_WAREHOUSE_NAME,
        W_WAREHOUSE_SK,
        I_ITEM_SK,
        D_MOY,
        STDEV,
        MEAN,
        CASE WHEN MEAN = 0 THEN NULL ELSE STDEV / MEAN END AS COV
    FROM MV_INV
    WHERE D_YEAR = 2002 AND (MEAN = 0 OR STDEV / MEAN > 1)
)
SELECT
    INV1.W_WAREHOUSE_SK,
    INV1.I_ITEM_SK,
    INV1.D_MOY,
    INV1.MEAN,
    INV1.COV,
    INV2.W_WAREHOUSE_SK,
    INV2.I_ITEM_SK,
    INV2.D_MOY,
    INV2.MEAN,
    INV2.COV
FROM INV INV1, INV INV2
WHERE
    INV1.I_ITEM_SK = INV2.I_ITEM_SK
    AND INV1.W_WAREHOUSE_SK = INV2.W_WAREHOUSE_SK
    AND INV1.D_MOY = 4
    AND INV2.D_MOY = 4 + 1
ORDER BY
    INV1.W_WAREHOUSE_SK,
    INV1.I_ITEM_SK,
    INV1.D_MOY,
    INV1.MEAN,
    INV1.COV,
    INV2.D_MOY,
    INV2.MEAN,
    INV2.COV;
    
WITH INV AS (
    SELECT 
        W_WAREHOUSE_NAME,
        W_WAREHOUSE_SK,
        I_ITEM_SK,
        D_MOY,
        STDEV,
        MEAN,
        CASE WHEN MEAN = 0 THEN NULL ELSE STDEV / MEAN END AS COV
    FROM MV_INV
    WHERE D_YEAR = 2002 AND (MEAN = 0 OR STDEV / MEAN > 1)
)
SELECT
    INV1.W_WAREHOUSE_SK,
    INV1.I_ITEM_SK,
    INV1.D_MOY,
    INV1.MEAN,
    INV1.COV,
    INV2.W_WAREHOUSE_SK,
    INV2.I_ITEM_SK,
    INV2.D_MOY,
    INV2.MEAN,
    INV2.COV
FROM INV INV1, INV INV2
WHERE
    INV1.I_ITEM_SK = INV2.I_ITEM_SK
    AND INV1.W_WAREHOUSE_SK = INV2.W_WAREHOUSE_SK
    AND INV1.D_MOY = 4
    AND INV2.D_MOY = 4 + 1
    AND INV1.COV > 1.5
ORDER BY
    INV1.W_WAREHOUSE_SK,
    INV1.I_ITEM_SK,
    INV1.D_MOY,
    INV1.MEAN,
    INV1.COV,
    INV2.D_MOY,
    INV2.MEAN,
    INV2.COV;