LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\call_center.csv' 
        INTO TABLE call_center 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\catalog_page.csv' 
        INTO TABLE catalog_page 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\catalog_returns.csv' 
        INTO TABLE catalog_returns 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\catalog_sales.csv' 
        INTO TABLE catalog_sales 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\customer.csv' 
        INTO TABLE customer 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\customer_address.csv' 
        INTO TABLE customer_address 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\customer_demographics.csv' 
        INTO TABLE customer_demographics 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\date_dim.csv' 
        INTO TABLE date_dim 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\household_demographics.csv' 
        INTO TABLE household_demographics 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\income_band.csv' 
        INTO TABLE income_band 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\inventory.csv' 
        INTO TABLE inventory 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\item.csv' 
        INTO TABLE item 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\promotion.csv' 
        INTO TABLE promotion 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\reason.csv' 
        INTO TABLE reason 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\ship_mode.csv' 
        INTO TABLE ship_mode 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\store.csv' 
        INTO TABLE store 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\store_returns.csv' 
        INTO TABLE store_returns 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\store_sales.csv' 
        INTO TABLE store_sales 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\time_dim.csv' 
        INTO TABLE time_dim 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\warehouse.csv' 
        INTO TABLE warehouse 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\web_page.csv' 
        INTO TABLE web_page 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\web_returns.csv' 
        INTO TABLE web_returns 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\web_sales.csv' 
        INTO TABLE web_sales 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'E:\\Documents\\BDMA\\ULB\\Data Warehouses\\project1\\DSGen-software-code-3.2.0rc1\\data\\1_nulls\\web_site.csv' 
        INTO TABLE web_site 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';