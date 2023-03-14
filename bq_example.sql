-- AUTHOR: HAYYU HANIFAH
-- QUERY: BANGKIT - DATA ANALYTICS

--===========================================================================--

--- MAKE SURE WHICH COLUMN IS THE PRIMARY KEY IN EACH TABLE ---
--- WITHIN 1 TABLE, THE VALUE OF PRIMARY KEY MUST BE UNIQUE (ONLY 1 ROW) ---

-- OPTION 1: Using aggregate function to check the number of row of each id value, then apply descending order to the number of row so if there is any id with > 1 row it will appear at the top
SELECT order_id, 
       COUNT(1) AS n_row
FROM `bigquery-public-data.thelook_ecommerce.orders`
GROUP BY 1
ORDER BY 2 DESC;

-- OPTION 2: Similar with option 1, however we will use HAVING() function to check whether there is any id with > 1 row. If this query return no data, then the column is primary key.
SELECT order_id,
       COUNT(1) AS n_row
FROM `bigquery-public-data.thelook_ecommerce.orders`
GROUP BY 1
HAVING COUNT(1) > 1;

--===========================================================================--

--- DOING SOME DATA EXPLORATION ---

-- Check the possible value of event_type, and how many users perform each events on the ecommerce site
SELECT event_type, 
       COUNT(id) AS n_event,
       COUNT(user_id) AS n_user_id,
       COUNT(DISTINCT user_id) AS n_user_id_unique
FROM `bigquery-public-data.thelook_ecommerce.events`
GROUP BY 1
order by 2;

-- Check the possible value for product department, category
SELECT department,
       category,
       COUNT(DISTINCT id) AS n_product,
       MIN(retail_price) AS min_retail_price,
       MAX(retail_price) AS max_retail_price,
       AVG(retail_price) AS avg_retail_price,
       APPROX_QUANTILES(retail_price,4)[OFFSET(2)] AS median_retail_price
FROM `bigquery-public-data.thelook_ecommerce.products`
GROUP BY 1,2
ORDER BY 1,2;

-- Check users' demography (country, gender, age)
SELECT country,
       gender,
       COUNT(id) AS n_users,
       MIN(age) AS min_age,
       MAX(age) AS max_age,
       AVG(age) AS avg_age,
       APPROX_QUANTILES(age,4)[OFFSET(2)] AS median_age,
FROM `bigquery-public-data.thelook_ecommerce.users`
GROUP BY 1,2
ORDER BY 1,2;

-- Check the status of orders made during February 2023
SELECT status,
       COUNT(order_id) AS n_orders
FROM `bigquery-public-data.thelook_ecommerce.orders`
-- WHERE DATE(created_at) BETWEEN DATE('2023-02-01') AND DATE('2023-02-28')
WHERE DATE_TRUNC(DATE(created_at),MONTH) = DATE('2023-02-01')
GROUP BY 1
ORDER BY 2 DESC;

--===========================================================================--

--- DOING DATA SUMMARIZATION ---

-- users geographical distribution + gender + age -- user created from Jan 2019 until now -> demonstrate cumulative summary
WITH monthly_users AS (
  SELECT DATE_TRUNC(DATE(created_at), MONTH) AS reporting_month,
         gender, 
         country,
         COUNT(id) AS n_user
  FROM `bigquery-public-data.thelook_ecommerce.users`
  GROUP BY 1,2,3
)
, monthly_users_cumulative AS (
  SELECT reporting_month,
        gender,
        country,
        n_user,
        SUM(n_user) OVER(PARTITION BY gender, country ORDER BY reporting_month) AS cumulative_n_user
  FROM monthly_users
  ORDER BY 1,2,3
)
SELECT *
FROM monthly_users_cumulative;
-- WHERE country = 'United Kingdom'
-- AND gender = 'M'

-- transactions volume geographical distribution, monthly
WITH 
-- adding information about users
orders_x_users AS ( 
  SELECT orders.*,
         users.country AS users_country,
         users.gender AS users_gender
  FROM `bigquery-public-data.thelook_ecommerce.orders` AS orders
  LEFT JOIN `bigquery-public-data.thelook_ecommerce.users` AS users
  ON orders.user_id = users.id
)
, monthly_orders_distribution AS (
  SELECT DATE_TRUNC(DATE(created_at),MONTH) AS reporting_month,
         users_country,
         users_gender,
         COUNT(DISTINCT order_id) AS n_order,
         COUNT(DISTINCT user_id) AS n_purchasers -- user who made orders
  FROM orders_x_users
  GROUP BY 1,2,3
  ORDER BY 1,2,3
)
SELECT *
FROM monthly_orders_distribution;

-- Monthly transaction of each product category
WITH
-- add information about products ordered by the users
orders_x_order_items AS (
  SELECT orders.*,
         order_items.inventory_item_id,
         order_items.sale_price
  FROM `bigquery-public-data.thelook_ecommerce.orders` AS orders
  LEFT JOIN `bigquery-public-data.thelook_ecommerce.order_items` AS order_items
  ON orders.order_id = order_items.order_id
)
, orders_x_inventory AS (
  SELECT orders_x_order_items.*,
         inventory_items.product_category,
         inventory_items.product_department,
         inventory_items.product_retail_price,
         inventory_items.product_distribution_center_id,
         inventory_items.cost,
         distribution_centers.name
  FROM orders_x_order_items
  LEFT JOIN `bigquery-public-data.thelook_ecommerce.inventory_items` AS inventory_items
  ON orders_x_order_items.inventory_item_id = inventory_items.id
  LEFT JOIN `bigquery-public-data.thelook_ecommerce.distribution_centers` AS distribution_centers
  ON inventory_items.product_distribution_center_id = distribution_centers.id
)
, orders_x_users AS (
  SELECT orders_x_inventory.*,
         users.country AS users_country,
  FROM orders_x_inventory 
  LEFT JOIN `bigquery-public-data.thelook_ecommerce.users` AS users
  ON orders_x_inventory.user_id = users.id
)
, monthly_order_product_category AS (
  SELECT DATE_TRUNC(DATE(created_at),MONTH) AS reporting_month,
         users_country,
         product_department,
         product_category, -- 1 order might consist of > 1 categories
         COUNT(DISTINCT order_id) AS n_order,
         COUNT(DISTINCT user_id) AS n_purchasers,
         SUM(product_retail_price) AS total_product_retail_price,
         SUM(cost) AS total_cost
  FROM orders_x_users
  GROUP BY 1,2,3,4
  ORDER BY 1,2,3,4
)
SELECT *,
       total_product_retail_price - total_cost AS total_profit
FROM monthly_order_product_category;
