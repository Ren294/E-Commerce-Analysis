CREATE DATABASE IF NOT EXISTS ecom;

-- CREATE TABLE IF NOT EXISTS ecom.dim_customers (
--     customer_id STRING,
--     customer_unique_id STRING,
--     customer_zip_code_prefix STRING,
--     customer_city STRING,
--     customer_state STRING,
--     geolocation_lat DOUBLE,
--     geolocation_lng DOUBLE
-- );

-- CREATE TABLE IF NOT EXISTS ecom.dim_geolocation (
--     geolocation_zip_code_prefix STRING,
--     geolocation_lat DOUBLE,
--     geolocation_lng DOUBLE,
--     geolocation_city STRING,
--     geolocation_state STRING
-- );

-- CREATE TABLE IF NOT EXISTS ecom.dim_product_category (
--     product_category_name STRING,
--     product_category_name_english STRING
-- );

-- CREATE TABLE IF NOT EXISTS ecom.dim_products (
--     product_id STRING,
--     product_category_name STRING,
--     product_category_name_english STRING,
--     product_name_length INT,
--     description_length INT,
--     product_photos_qty INT,
--     product_weight_g FLOAT,
--     product_length_cm FLOAT,
--     product_height_cm FLOAT,
--     product_width_cm FLOAT
-- );

-- CREATE TABLE IF NOT EXISTS ecom.dim_sellers (
--     seller_id STRING,
--     seller_zip_code_prefix STRING,
--     seller_city STRING,
--     seller_state STRING,
--     geolocation_lat DOUBLE,
--     geolocation_lng DOUBLE
-- );

-- CREATE TABLE IF NOT EXISTS ecom.fact_order_item (
--     order_id STRING,
--     order_item_id INT,
--     product_id STRING,
--     product_category_name STRING,
--     seller_id STRING,
--     seller_city STRING,
--     seller_state STRING,
--     customer_id STRING,
--     order_status STRING,
--     order_purchase_timestamp TIMESTAMP,
--     shipping_limit_date TIMESTAMP,
--     price DOUBLE,
--     freight_value DOUBLE,
--     payment_type STRING,
--     payment_installments INT,
--     payment_value DOUBLE,
--     review_score INT,
--     review_comment_message STRING
-- );

-- CREATE TABLE IF NOT EXISTS ecom.fact_orders (
--     order_id STRING,
--     customer_id STRING,
--     seller_id STRING,
--     product_id STRING,
--     order_status STRING,
--     order_purchase_timestamp TIMESTAMP,
--     order_approved_at TIMESTAMP,
--     order_delivered_carrier_date TIMESTAMP,
--     order_delivered_customer_date TIMESTAMP,
--     order_estimated_delivery_date TIMESTAMP,
--     payment_type STRING,
--     payment_installments INT,
--     payment_value DOUBLE,
--     review_score INT
-- );