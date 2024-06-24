----------
SELECT COUNT(*) AS flight_quantity
FROM aeropuerto_tabla
WHERE fecha BETWEEN '2021-12-01' AND '2022-01-31';
----------
SELECT SUM(pasajeros) AS total_passengers
FROM aeropuerto_tabla
WHERE aerolinea_nombre = 'AEROLINEAS ARGENTINAS SA'
AND fecha BETWEEN '2021-01-01' AND '2022-06-30';
----------
SELECT 
    at.fecha AS date, 
    at.horaUTC AS time, 
    at.tipo_de_movimiento AS movement_type, 
    at.aeropuerto AS departure_airport_code, 
    dep.denominacion AS departure_city, 
    at.origen_destino AS arrival_airport_code, 
    arr.denominacion AS arrival_city,     
    at.pasajeros AS number_of_passengers
FROM aeropuerto_tabla AS at
LEFT JOIN aeropuerto_detalles_tabla AS dep ON at.aeropuerto = dep.aeropuerto
LEFT JOIN aeropuerto_detalles_tabla AS arr ON at.origen_destino = arr.aeropuerto
WHERE at.fecha BETWEEN '2022-01-01' AND '2022-06-30'
  AND (at.tipo_de_movimiento = 'Despegue' OR at.tipo_de_movimiento = 'Aterrizaje')
ORDER BY at.fecha DESC, at.horaUTC DESC;
------
SELECT 
    aerolinea_nombre AS airline_name,
    SUM(pasajeros) AS total_passengers
FROM aeropuerto_tabla
WHERE fecha BETWEEN '2021-01-01' AND '2022-06-30'
  AND aerolinea_nombre IS NOT NULL 
  AND aerolinea_nombre != '0'
GROUP BY aerolinea_nombre
ORDER BY total_passengers DESC
LIMIT 10;
-----
SELECT 
    at.aeronave AS aircraft_name,
    COUNT(*) AS flight_count
FROM aeropuerto_tabla AS at
INNER JOIN aeropuerto_detalles_tabla AS dep ON at.aeropuerto = dep.aeropuerto
INNER JOIN aeropuerto_detalles_tabla AS arr ON at.origen_destino = arr.aeropuerto
WHERE at.fecha BETWEEN '2021-01-01' AND '2022-06-30'
  AND (dep.provincia = 'BUENOS AIRES' OR arr.provincia = 'BUENOS AIRES' OR 
       dep.provincia = 'CIUDAD AUTÓNOMA DE BUENOS AIRES' OR arr.provincia 
       = 'CIUDAD AUTÓNOMA DE BUENOS AIRES')
  AND at.aeronave IS NOT NULL AND at.aeronave != '0'
  AND (at.tipo_de_movimiento = 'Despegue')
GROUP BY at.aeronave
ORDER BY flight_count DESC
LIMIT 10;

