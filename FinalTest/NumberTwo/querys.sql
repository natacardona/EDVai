SELECT 
    COUNT(*) AS number_of_rentals
FROM car_rental_analytics
WHERE (fuelType = 'HYBRID' OR fuelType = 'ELECTRIC')
  AND rating >= 4;

------------

SELECT 
    state_name,
    COUNT(*) AS rental_count
FROM car_rental_analytics
GROUP BY state_name
ORDER BY rental_count ASC
LIMIT 5;

-----

SELECT 
    make AS brand,
    model,
    COUNT(*) AS rental_count
FROM car_rental_analytics
GROUP BY make, model
ORDER BY rental_count DESC
LIMIT 10;

-----

SELECT 
    year AS manufacture_year,
    COUNT(*) AS rental_count
FROM car_rental_analytics
WHERE year BETWEEN 2010 AND 2015
GROUP BY year
ORDER BY year;

----

SELECT 
    city,
    COUNT(*) AS rental_count
FROM car_rental_analytics
WHERE fuelType IN ('HYBRID', 'ELECTRIC')
GROUP BY city
ORDER BY rental_count DESC
LIMIT 5;

----

SELECT 
    fuelType,
    AVG(reviewCount) AS average_reviews,
    SUM(reviewCount) AS total_reviews
FROM car_rental_analytics
WHERE fuelType IN ('GASOLINE', 'HYBRID', 'ELECTRIC', 'DIESEL')
  AND reviewCount IS NOT NULL
GROUP BY fuelType
ORDER BY average_reviews DESC;

