create database fligthsdb;

CREATE TABLE vuelos (
fecha DATE,
horaUTC varchar(500),
clase_de_vuelo varchar(500),
clasificacion_de_vuelo varchar(500),
tipo_de_movimiento varchar(500),
aeropuerto varchar(500),
origen_destino varchar(500),
aerolinea_nombre varchar(500),
aeronave varchar(500),
pasajeros INT
);


CREATE TABLE detalle_aeropuertos (
aeropuerto varchar(500),
oac varchar(500),
iata varchar(500),
tipo varchar(500),
denominacion varchar(500),
coordenadas varchar(500),
latitud varchar(500),
longitud varchar(500),
elev FLOAT,
uom_elev varchar(500),
ref varchar(500),
distancia_ref FLOAT,
direccion_ref varchar(500),
condicion varchar(500),
control varchar(500),
region varchar(500),
uso varchar(500));
