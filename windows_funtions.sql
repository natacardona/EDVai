--Get the average price for each product category. The clause OVER(PARTITION BY CategoryID) specifies that the average price should be calculated for each unique CategoryID value in the table.
SELECT C.category_name,
       P.product_name,
       P.unit_price,
       Avg(P.unit_price)
         OVER(
           partition BY P.category_id) AS AvgPriceByCategory
FROM   products P
       JOIN categories C
         ON P.category_id = C.category_id;
         