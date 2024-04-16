/* AVG
1.Get the average price for each product category. The clause OVER(PARTITION BY CategoryID) specifies that the average price should be calculated for each unique CategoryID value in the table.*/
SELECT C.category_name,
       P.product_name,
       P.unit_price,
       Avg(P.unit_price)
         OVER(
           partition BY P.category_id) AS AvgPriceByCategory
FROM   products P
       JOIN categories C
         ON P.category_id = C.category_id;

/* 2.Obtain the average sales of each client */
SELECT o.order_id,
       o.customer_id,
       o.employee_id,
       o.order_date,
       o.required_date,
       o.shipped_date,
       Avg(od.unit_price * od.quantity * ( 1 - COALESCE(od.discount, 0) ))
         OVER (
           partition BY o.customer_id) AS AvgOrderAmount
FROM   orders o
       JOIN order_details od
         ON o.order_id = od.order_id;
/* 3. Get the average quantity of products sold by category (product_name, quantity_per_unit, unit_price, quantity, avgquantity) and sort it by category name and product name */
SELECT P.product_name,
       c.category_name,
       P.quantity_per_unit,
       P.unit_price,
       OD.quantity,
       Avg(OD.quantity)
         OVER (
           partition BY c.category_name) AS avgquantity
FROM   products P
       INNER JOIN order_details OD
               ON P.product_id = OD.product_id
       INNER JOIN categories C
               ON P.category_id = C.category_id
ORDER  BY c.category_name,
          P.product_name; 
/*MIN 
4.Selects the customer ID, order date, and oldest order date for each customer from the 'Orders' table */
SELECT customer_id,
       order_date,
       Min(order_date)
         OVER (
           partition BY customer_id) AS earliestorderdate
FROM   orders
ORDER BY customer_id,
          order_date; 

/*MAX 
5. Seleccione el id de producto, el nombre de producto, el precio unitario, el id de categoría y el precio unitario máximo para cada categoría de la tabla Products. */
SELECT product_id,
       product_name,
       unit_price,
       category_id,
       Max(unit_price)
         OVER (
           partition BY category_id) AS maxunitprice
FROM   products; 
/*Row_number
6. Obtain the ranking of the best-selling products*/
SELECT Row_number()
         OVER (
           ORDER BY Sum(quantity) DESC) AS ranking,
       product_name,
       Sum(quantity)                    AS totalquantity
FROM   order_details
       JOIN products
         ON order_details.product_id = products.product_id
GROUP  BY product_name
ORDER  BY totalquantity DESC; 

/* 
7.Assign row numbers for each customer, sorted by customer_id*/
SELECT Row_number()
         OVER (
           ORDER BY customer_id) AS rownumber,
       customer_id,
       company_name,
       contact_name,
       contact_title,
       address
FROM   customers
ORDER  BY customer_id; 
/* 
8. Get the ranking of the youngest employees (ranking, first and last name of the employee, date of birth) */
SELECT Row_number()
         OVER (
           ORDER BY birth_date DESC)      AS ranking,
       Concat(first_name, ' ', last_name) AS employeename,
       birth_date
FROM   employees
ORDER  BY birth_date DESC;
/* SUM
9. Obtain the sales amount of each customer*/
SELECT o.order_id,
       o.customer_id,
       o.employee_id,
       o.order_date,
       o.required_date,
       Sum(od.unit_price * od.quantity)
         OVER (
           partition BY o.customer_id) AS sumorderamount
FROM   orders o
       INNER JOIN order_details od
               ON o.order_id = od.order_id
GROUP  BY o.order_id,
          o.customer_id,
          o.employee_id,
          o.order_date,
          o.required_date,
          od.unit_price,
          od.quantity;

/*
10.Obtain the total sum of sales by product category*/
SELECT c.category_name,
       p.product_name,
       p.unit_price,
       od.quantity,
       Sum(p.unit_price * od.quantity * ( 1 - COALESCE(od.discount, 0) ))
         OVER (
           partition BY c.category_name) AS TotalSales
FROM   products p
       JOIN order_details od
         ON p.product_id = od.product_id
       JOIN categories c
         ON p.category_id = c.category_id
GROUP  BY c.category_name,
          p.product_name,
          p.unit_price,
          od.quantity,
          od.discount
ORDER  BY c.category_name,
          p.product_name;

/*11. Calculate the total amount of shipping costs per destination country, then sort it by country and by ascending order*/
SELECT ship_country                   AS country,
       order_id                       AS order_id,
       shipped_date                   AS shipped_date,
       freight                        AS freight,
       Sum(freight)
         OVER (
           partition BY ship_country) AS TotalShippingCosts
FROM   orders
WHERE  shipped_date IS NOT NULL
ORDER  BY ship_country ASC,
          order_id ASC; 
/* RANK
12.Sales ranking by customer*/
SELECT c.customer_id,
       c.company_name,
       Sum(od.unit_price * od.quantity * ( 1 - od.discount ))
       AS TotalSales,
       Rank()
         OVER (
           ORDER BY Sum(od.unit_price * od.quantity * (1 - od.discount)) DESC)
       AS Rank
FROM   customers c
       JOIN orders o
         ON c.customer_id = o.customer_id
       JOIN order_details od
         ON o.order_id = od.order_id
GROUP  BY c.customer_id,
          c.company_name
ORDER  BY totalsales DESC; 

/*13.Ranking of employees by hiring date*/
SELECT 
  employee_id ,
  first_name ,
  last_name ,
  hire_date ,
  RANK() OVER (ORDER BY hire_date  ASC) AS Rank
FROM 
  Employees
ORDER BY 
  hire_date  ASC;
 
/*14.Ranking of products by unit price*/
SELECT product_id,
       product_name,
       unit_price,
       Rank()
         OVER (
           ORDER BY unit_price DESC) AS Rank
FROM   products
ORDER  BY unit_price DESC; 
/*LAG
15.Show for each product in an order, the quantity sold and the quantity sold of the previous product.*/

SELECT product_id,
       product_name,
       unit_price,
       Rank()
         OVER (
           ORDER BY unit_price DESC) AS Rank
FROM   products
ORDER  BY unit_price DESC; 
 /*16.Get a list of orders showing the order id, order date, customer id and last order date*/
SELECT order_id,
       order_date,
       customer_id,
       Lag(order_date)
         OVER (
           partition BY customer_id
           ORDER BY order_date) AS lastorderdate
FROM   orders
ORDER  BY customer_id,
          order_date; 
/*17.Obtain a list of products that contain: product id, product name, unit price, price of the previous product, difference between the price of the product and price of the previous product. 
*/
SELECT product_id,
       product_name,
       unit_price,
       Lag(unit_price)
         OVER (
           ORDER BY product_id)              AS lastunitprice,
       unit_price - Lag(unit_price)
                      OVER (
                        ORDER BY product_id) AS pricedifference
FROM   products
ORDER  BY product_id; 

/*LEAD
18.Get a listing showing the price of one product along with the price of the next product:*/
SELECT product_name,
       unit_price,
       Lead(unit_price)
         OVER (
           ORDER BY product_id) AS NextPrice
FROM   products
ORDER  BY product_id; 

/*19.Get a listing showing the total sales per product category along with the total sales of the next category
Review */
WITH categorysales
     AS (SELECT c.category_name,
                Sum(od.unit_price * od.quantity * ( 1 - COALESCE(od.discount, 0)
                                                  ))
                AS
                TotalSales
         FROM   categories c
                JOIN products p
                  ON c.category_id = p.category_id
                JOIN order_details od
                  ON p.product_id = od.product_id
         GROUP  BY c.category_name)
SELECT category_name,
       totalsales,
       Lead(totalsales)
         OVER (
           ORDER BY category_name) AS NextTotalSales
FROM   categorysales
ORDER  BY category_name; 
