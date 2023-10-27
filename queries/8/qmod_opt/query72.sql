select  i_item_desc
      ,w_warehouse_name
      ,d1.d_week_seq
      ,sum(case when p_promo_sk is null then 1 else 0 end) no_promo
      ,sum(case when p_promo_sk is not null then 1 else 0 end) promo
      ,count(*) total_cnt
from catalog_sales
join inventory on (cs_item_sk = inv_item_sk and inv_quantity_on_hand < cs_quantity )
join warehouse on (w_warehouse_sk=inv_warehouse_sk)
join item on (i_item_sk = cs_item_sk)
join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)
join date_dim d2 on (inv_date_sk = d2.d_date_sk and d1.d_week_seq = d2.d_week_seq)
join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk and d3.d_date > d1.d_date + 5)
left outer join promotion on (cs_promo_sk=p_promo_sk)
where  
  d1.d_year = 2002
  AND EXISTS (SELECT 1 FROM HOUSEHOLD_DEMOGRAPHICS JOIN catalog_sales ON CS_BILL_HDEMO_SK = HD_DEMO_SK WHERE HD_BUY_POTENTIAL = '>10000' LIMIT 1)
  AND EXISTS (SELECT 1 FROM CUSTOMER_DEMOGRAPHICS join catalog_sales ON CS_BILL_CDEMO_SK = CD_DEMO_SK where CD_MARITAL_STATUS = 'U' LIMIT 1)
group by i_item_desc,w_warehouse_name,d1.d_week_seq
order by total_cnt desc, i_item_desc, w_warehouse_name, d_week_seq
limit 100;