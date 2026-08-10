--> What is an index? <--
-- An index in SQL is a data structure (like a book's index) that allows the database engine to quickly locate and retrieve rows without scanning the entire table. It's usually implemented as a B-Tree or
-- Hash structure depending on the database engine. 

--> How indexing works <--
-- When a query runs with a WHERE, JOIN, or ORDER BY clause, SQL looks up the index instead of
-- Scanning all rows sequentially (a full table scan).
--> without using index :

SELECT * FROM refined_layer.sales -- Without an index: PostgreSQL has to read the table row by row to check which ones have country_code = 'US'. This process is called a full table scan.If your table has 10,000 rows, that’s still manageable.
WHERE country_code = 'US'; --But if the table grows to 10 million rows (after months of daily runs), each query will become slower and slower because PostgreSQL must scan every single row before returning the result.


-- to show the process of this  full scan: the QUERY PLAN  
EXPLAIN ANALYZE
SELECT * FROM refined_layer.sales
WHERE country_code = 'US';  --Seq Scan = sequential scan = full table scan (slow on large table).

-- Seq Scan on sales  (cost=0.00..2.75 rows=1 width=285) (actual time=0.022..0.042 rows=5 loops=1)

-- AFTER index :   
CREATE INDEX idx_refined_sales_country
ON refined_layer.sales (country_code);


EXPLAIN ANALYZE
SELECT * FROM refined_layer.sales
WHERE country_code = 'US';

 -- Index Scan using idx_refined_sales_country on sales

CREATE INDEX mon_super_index ON raw_layer.sales (ingestion_timestamp);

EXPLAIN ANALYZE
SELECT * 
FROM raw_layer.sales
ORDER BY ingestion_timestamp;




-- Sample query to visualise my data

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

--- without using the Widow function ,just the GROUP: ligne per ligne 
select customer_name,SUM(unit_price) 
FROM raw_layer.sales 
group by customer_name

--- total_amount per country_code
-- GROUP BY : une seule ligne par pays
SELECT customer_name,country_code, SUM(total_amount) AS total
FROM refined_layer.sales
GROUP BY country_code,customer_name;

-- Window function : garde CHAQUE commande, ajoute le total du pays à côté
SELECT
    customer_name,
    country_code,
    SUM(total_amount) OVER (PARTITION BY country_code) AS country_total
FROM refined_layer.sales;


-- For each country, what is the revenue ranking of each product 
SELECT
    country_code,
    product_id,
    total_amount,
    RANK() OVER (
        PARTITION BY country_code
        ORDER BY total_amount DESC
    ) AS revenue_rank_in_country
FROM refined_layer.sales
WHERE is_valid_transaction = TRUE;


 
SELECT CURRENT_DATE; --- for selecting the current date 


SUM(total_amount) OVER (ORDER BY order_date)


SELECT
    order_date,
    SUM(total_amount) OVER (ORDER BY order_date) AS cumulative_revenue
FROM refined_layer.sales
WHERE is_valid_transaction = TRUE;





 -- customer  name that duplicate
 SELECT customer_name, COUNT(*) AS x from raw_layer.sales
 WHERE x>1
 GROUP BY customer_name



-- 
SELECT *
FROM raw_layer.sales
WHERE order_id IN (
    SELECT order_id
    FROM raw_layer.sales
    GROUP BY order_id
    HAVING COUNT(*) > 1
)
ORDER BY order_id, order_date;

-- Count how many countries have more than 5 valid orders: using a inner query  and then a outer query 

SELECT COUNT(*) AS country_count
FROM (
    SELECT country_code
    FROM refined_layer.sales
    WHERE is_valid_transaction = TRUE
    GROUP BY country_code
    HAVING COUNT(*) > 5
) AS subquery;

-- in other  more optimize with returning the country_code
SELECT country_code 
FROM refined_layer.sales
WHERE is_valid_transaction = TRUE
GROUP BY country_code
HAVING COUNT(*) > 5;


-- using a CTE (common table expression) with the famous "WITH ... AS " syntaxe

WITH valid_countries AS(
    SELECT country_code
    FROM refined_layer.sales
    WHERE is_valid_transaction = TRUE
    GROUP BY country_code
    HAVING COUNT(*) > 5
) 
SELECT COUNT(DISTINCT country_code) AS country_count
FROM valid_countries ;


-- Retrieve orders with amounts above the average order value
SELECT *
FROM refined_layer.sales
WHERE total_amount > (SELECT AVG(total_amount) FROM refined_layer.sales);

-- data quality check  : finds order with a non-positive quantity or price
SELECT * 
FROM raw_layer.sales

--- Reminder	—	the	core	idea:	unlike	GROUP	BY,	which	collapses	multiple
-- rows	into	one,	a	window	function	keeps	every	original	row	and	adds	a
-- calculated	column	based	on	a	“window”	of	related	rows	(defined	by	
-- PARTITION	BY	and/or	ORDER	BY).

-- ROW_NUMBER():assigns a unique sequential number to each row 

SELECT	
  customer_id,	
  order_id,	
  order_date,
  ROW_NUMBER()	OVER	(PARTITION	BY	customer_id	)	AS	rn
FROM	refined_layer.sales

--- using a cte and return just customer  where the rn different then 1

WITH ranked AS(
SELECT	
  customer_id,	
  order_id,	
  order_date,
  ROW_NUMBER()	OVER	(PARTITION	BY	customer_id	)	AS	rn
FROM	refined_layer.sales

)
SELECT * from ranked where rn!=1;

-- Ranks	rows,	leaving	gaps	after	ties	(1,	2,	2,	4…)
SELECT
  payment_method,
  customer_id,	
  order_id,	
  order_date,
  ROW_NUMBER()	OVER	(PARTITION BY payment_method )	AS	rn
FROM	refined_layer.sales

SELECT
  payment_method,
  customer_id,	
  order_id,	
  order_date,
  RANK()	OVER	(PARTITION BY payment_method )	AS	rn
FROM	refined_layer.sales





SELECT country_code , order_id , total_amount,
       ROW_NUMBER() OVER(PARTITION BY country_code ORDER BY total_amount DESC) as revenue_rank
FROM refined_layer.sales
WHERE is_valid_transaction=TRUE 


SELECT country_code , order_id , total_amount,
       RANK() OVER(PARTITION BY country_code ORDER BY total_amount DESC) as revenue_rank
FROM refined_layer.sales
WHERE is_valid_transaction=TRUE 


-- DENSE_RANK() runn after the 

SELECT country_code , order_id , total_amount,
       DENSE_RANK() OVER(PARTITION BY country_code ORDER BY total_amount DESC) as revenue_rank
FROM refined_layer.sales
WHERE revenue_rank=2;
	   
	   

-- Sans ORDER BY : ordre arbitraire, rn=1 ne veut rien dire de précis
SELECT customer_id, order_id, order_date,
       ROW_NUMBER() OVER (PARTITION BY customer_id) AS rn
FROM refined_layer.sales
ORDER BY customer_id;

-- Avec ORDER BY : rn=1 = commande la plus récente, garanti
SELECT customer_id, order_id, order_date,
       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
FROM refined_layer.sales
ORDER BY customer_id, rn;





SELECT 
   customer_id,
   order_id,
   RANK() OVER(ORDER BY order_id DESC) as rn
FROM raw_layer.sales


--- this explain the diffrence between RANK adn DENSE_RANK : 
---> RANK : assigns the same rank to equals values and skips the next rank numbers
SELECT 
   customer_id,
   quantity ,
   RANK() OVER(ORDER BY quantity DESC  ) as rn
FROM raw_layer.sales

---> DENSE_RANK() assigns the same rank to equal values but does not skip numbers
SELECT 
   customer_id,
   quantity ,
   DENSE_RANK() OVER(ORDER BY quantity DESC  ) as rn
FROM raw_layer.sales



-- NTILE(n) Splits	rows	into	n	roughly	equal	buckets,	numbered	1	to	n.

SELECT	
    order_id,
    total_amount,
NTILE(10)	OVER	(ORDER	BY	total_amount	DESC)	AS	decile
FROM	refined_layer.sales
WHERE	is_valid_transaction	=	TRUE;
--- LAG(column,	offset,	default) 
--- Returns	the	value	of	column	from	a	previous	row	(default	offset	=	1)
--- Within	the	partition.

---> Usage:	compare	each	order’s	amount	to	the	customer’s	previous order	—	detect	growing	spend.
	
WITH cte AS(
   SELECT customer_id , order_date,total_amount,
       LAG(total_amount) OVER (ORDER BY order_date ) AS prv_amount
	FROM refined_layer.sales
)
SELECT	*	FROM	cte  	WHERE	total_amount	< prv_amount;	


-- LEAD(column,	offset,	default)
-- The	mirror	of	LAG()	—	returns	the	value	from	a	following	row.

-- Usage:	for	each	order,	see	the	amount	of	the	customer’s	next	order,	to
-- compute	the	number	of	days	until	their	next	purchase.

SELECT	
    customer_id,
	order_date,
    LEAD(order_date)	OVER	(PARTITION	BY	customer_id	ORDER	BY order_date)	AS	next_order_date
FROM	refined_layer.sales;

-- FIRST_VALUE(column)
-- Returns	the	value	of	column	from	the	first	row	of	the	window	frame
-- Usage:	attach	each	customer’s	first-ever	order	amount	to	every	one	of
-- their	rows	(useful	to	compare	“first	order	size”	vs	“current	order size”).
SELECT	
  customer_id,	
  order_id,	
  order_date,
  total_amount,
  FIRST_VALUE(total_amount)	OVER	( PARTITION	BY	customer_id	ORDER	BY	order_date)	AS	first_order_amount
FROM	refined_layer.sales; 

-- 3.	Aggregate	functions	used	as	window functions
--> Any	standard	aggregate	(SUM,	AVG,	COUNT,	MIN,	MAX)	can	be	turned	into	a
--> window	function	simply	by	adding	OVER.
SELECT	
    order_date,
	SUM(total_amount)	OVER	(ORDER	BY	order_date)	AS	cumulative_revenue
FROM	refined_layer.sales
WHERE	is_valid_transaction	=	TRUE;


-->  AVG()	OVER	(...)	—	moving	average

SELECT	
    order_date,
	AVG(total_amount)	OVER	( ORDER	BY	order_date ROWS	BETWEEN	6	PRECEDING	AND	CURRENT	ROW) AS	moving_avg_7d
FROM	refined_layer.sales
WHERE	is_valid_transaction	=TRUE;


 

select * from raw_layer.sales;
select * from refined_layer.sales;
select * from report_layer.sales_by_country;
select * from report_layer.sales_by_payment_method;
select * from report_layer.sales_by_product;
SELECT AVG(total_amount) AS Avrage_amount
FROM refined_layer.sales
select product_id , product_name,COUNT(*) from report_layer.sales_by_product
group by product_id , product_name  
having count(*)>1;