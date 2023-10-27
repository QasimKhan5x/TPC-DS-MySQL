-- Indexes for date_dim table
CREATE INDEX idx_d_year ON date_dim(d_year);
CREATE INDEX idx_d_moy ON date_dim(d_moy);
CREATE INDEX idx_d_date ON date_dim(d_date);
CREATE INDEX idx_d_qoy ON date_dim(d_qoy);

-- Indexes for store_sales table
CREATE INDEX idx_ss_item_sk ON store_sales(ss_item_sk);
CREATE INDEX idx_ss_list_price ON store_sales(ss_list_price);
CREATE INDEX idx_ss_quantity ON store_sales(ss_quantity);
CREATE INDEX idx_ss_sold_date_sk ON store_sales(ss_sold_date_sk);

-- Indexes for web_sales table
CREATE INDEX idx_ws_quantity ON web_sales(ws_quantity);
CREATE INDEX idx_ws_ext_sales_price ON web_sales(ws_ext_sales_price);
CREATE INDEX idx_ws_sold_date_sk ON web_sales(ws_sold_date_sk);

-- Indexes for catalog_sales table
CREATE INDEX idx_cs_quantity ON catalog_sales(cs_quantity);
CREATE INDEX idx_cs_item_sk ON catalog_sales(cs_item_sk);
CREATE INDEX idx_cs_promo_sk ON catalog_sales(cs_promo_sk);
CREATE INDEX idx_cs_sold_date_sk ON catalog_sales(cs_sold_date_sk);

-- Indexes for inventory table
CREATE INDEX idx_inv_warehouse_sk ON inventory(inv_warehouse_sk);