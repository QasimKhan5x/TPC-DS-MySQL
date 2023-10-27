WITH CustomerTotalReturn AS (
    SELECT 
        sr_customer_sk AS ctr_customer_sk,
        sr_store_sk AS ctr_store_sk,
        SUM(SR_RETURN_AMT) AS ctr_total_return
    FROM store_returns
    JOIN date_dim ON sr_returned_date_sk = d_date_sk AND d_year = 2001
    WHERE sr_store_sk IN (SELECT s_store_sk FROM store WHERE s_state = 'TN') 
    GROUP BY sr_customer_sk, sr_store_sk
),

AvgTotalReturn AS (
    SELECT 
        ctr_store_sk,
        AVG(ctr_total_return) * 1.2 AS avg_total_return
    FROM CustomerTotalReturn
    GROUP BY ctr_store_sk
)

SELECT c_customer_id
FROM CustomerTotalReturn AS ctr1
JOIN AvgTotalReturn AS atr ON ctr1.ctr_store_sk = atr.ctr_store_sk
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
WHERE ctr1.ctr_total_return > atr.avg_total_return
ORDER BY c_customer_id
LIMIT 100;
