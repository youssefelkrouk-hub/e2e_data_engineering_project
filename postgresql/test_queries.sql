SELECT order_id, COUNT(*)
FROM raw_layer.sales
GROUP BY order_id
HAVING COUNT(*) > 1;
-- using a classique subquery withou cte : 
SELECT MAX(unit_price) as SecondHighestUnitprice
from raw_layer.sales 
where unit_price<(SELECT MAX(unit_price) from raw_layer.sales )
-- imple CTE(Common Table Expressions) with MAX (Cleanest)
WITH max_price AS (
      SELECT MAX(unit_price) AS  highest_price
	  FROM raw_layer.sales	  
)
SELECT MAX(unit_price) as SecondHighestUnitprice
FROM raw_layer.sales
WHERE unit_price<(SELECT highest_price FROM max_price)
-- the third highest unit price using subquery 
SELECT MAX(unit_price) as SecondHighestUnitprice
from raw_layer.sales 
where unit_price<(SELECT MAX(unit_price) as SecondHighestUnitprice
from raw_layer.sales 
where unit_price<(SELECT MAX(unit_price) from raw_layer.sales ))
-- the third highest unit price using the CTEs expression 


WITH highest AS ( -- highest CTE: Finds the maximum price (1st highest)
    SELECT MAX(unit_price) AS max_price
    FROM raw_layer.sales
),
second_highest AS ( -- second_highest CTE: Finds max price below the 1st highest (2nd highest)
    SELECT MAX(unit_price) AS second_max
    FROM raw_layer.sales
    WHERE unit_price < (SELECT max_price FROM highest)
)
SELECT MAX(unit_price) AS ThirdHighestUnitprice -- Main query: Finds max price below the 2nd highest (3rd highest)
FROM raw_layer.sales
WHERE unit_price < (SELECT second_max FROM second_highest);

-- By default, PostgreSQL "materializes" CTEs
-- This means it executes the CTE once and stores results
-- The CTE result is like a temporary table in memory

------ using th OFFSET and LIMIT key words
SELECT  DISTINCT unit_price
FROM raw_layer.sales
ORDER BY unit_price DESC
OFFSET 2 LIMIT 1
--- total_amount = quantity * unit_price ---
SELECT total_amount FROM refined_layer.sales
-- another way to calculate the tot_amount or total_revenue  using just the raw_layer.sales table 
SELECT product_id,
SUM(quantity*unit_price) as total_revenue
FROM  raw_layer.sales 
GROUP BY product_id

-- this next query will also return the total_amount , but 
-- the last we add the SUM aggregation funtion to  scan all the  raws   and no product-id is duplicated
SELECT product_id, quantity * unit_price AS revenue
FROM raw_layer.sales
---- Retrieve the second highest total_amount order using cte tehnique  
WITH max_amount as (
    SELECT MAX(total_amount) AS highest_price
	FROM refined_layer.sales
)
SELECT MAX(total_amount) as second_highest_total_amount
FROM refined_layer.sales
WHERE total_amount<(SELECT highest_price from max_amount)


-- COUNT(*) includes NULLs, but COUNT(order_id) does NOT include NULLs
SELECT  COUNT(*) 
from raw_layer.sales  -- return the number of raws
-- Customers who ordered but have no invalid transactions
SELECT DISTINCT  customer_id as customer_valid_transaction
FROM  refined_layer.sales
WHERE customer_id NOT IN (
    SELECT customer_id FROM refined_layer.sales WHERE is_valid_transaction=false 
) 

--Find products in raw_layer that never made it to refined_layer (Left Join usage)
SELECT r.product_id
FROM raw_layer.sales r
LEFT JOIN refined_layer.sales f
  ON r.order_id = f.order_id
WHERE f.order_id IS NULL;  -- this query return none raws this mean all the raw in the raw_layer table are transmeted to the refined_layer table  

-- Get the top 3 highest-revenue orders
SELECT order_id, total_amount
FROM refined_layer.sales
ORDER BY total_amount DESC
LIMIT 3;
-- Show the count of orders per customer
SELECT customer_id, COUNT(*) AS order_count
FROM refined_layer.sales
GROUP BY customer_id;

-- Retrieve all orders ingested today
SELECT *
FROM raw_layer.sales
WHERE ingestion_timestamp >= CURRENT_DATE;


--  Calculate the average order value per country
SELECT country_code,
       AVG(total_amount) AS avg_order_value
FROM refined_layer.sales
WHERE is_valid_transaction = TRUE
GROUP BY country_code;
-- Get the most recent order for each customer
SELECT customer_id, MAX(order_date) AS latest_order_date
FROM refined_layer.sales
GROUP BY customer_id;
-- Find products that were never marked valid
SELECT DISTINCT product_id
FROM refined_layer.sales
WHERE product_id NOT IN (
    SELECT product_id FROM refined_layer.sales WHERE is_valid_transaction = TRUE
);

-- Identify the best-selling product by quantity

SELECT product_id, SUM(quantity) AS total_qty
FROM refined_layer.sales
WHERE is_valid_transaction = TRUE
GROUP BY product_id
ORDER BY total_qty DESC
LIMIT 1;

--- Get total revenue and order count per payment method
SELECT payment_method,
       SUM(total_amount) AS total_revenue,
       COUNT(*) AS order_count
FROM refined_layer.sales
WHERE is_valid_transaction = TRUE
GROUP BY payment_method;
--- Retrieve orders with amounts above the average order value
SELECT *
FROM refined_layer.sales
WHERE total_amount > (SELECT AVG(total_amount) FROM refined_layer.sales);

-- Find all invalid transactions and their reason (cancelled/refunded status)
SELECT order_id, order_status, quantity, unit_price, email
FROM refined_layer.sales
WHERE is_valid_transaction = FALSE
  AND order_status IN ('cancelled', 'refunded');



select * from raw_layer.sales;
select * from refined_layer.sales;
select * from report_layer.sales_by_country;
select * from report_layer.sales_by_payment_method;
SELECT AVG(total_amount) AS Avrage_amount
FROM refined_layer.sales
select product_id , product_name,COUNT(*) from report_layer.sales_by_product
group by product_id , product_name  
having count(*)>1;