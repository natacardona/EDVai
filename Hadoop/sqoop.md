Ejercicios:

sqoop-me permite conectarme a mi ambiente de BD

1) Mostrar las tablas de la base de datos northwind

sqoop list-databases \
--connect jdbc:postgresql://172.17.0.3:5431/northwind \
--username postgres -P
# results
2024-05-21 18:50:06,102 INFO manager.SqlManager: Using default fetchSize of 1000
postgres
northwind
template1
template0

2) Mostrar los clientes de Argentina

sqoop eval \
--connect jdbc:postgresql://172.17.0.3:5431/northwind \
--username postgres \
--P \
--query "select * from customers where country = 'Argentina'"

# results
2024-05-21 18:44:38,582 INFO manager.SqlManager: Using default fetchSize of 1000
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| customer_id | company_name         | contact_name         | contact_title        | address              | city            | region          | postal_code | country         | phone                | fax                  | 
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| CACTU | Cactus Comidas para llevar | Patricio Simpson     | Sales Agent          | Cerrito 333          | Buenos Aires    | (null)          | 1010       | Argentina       | (1) 135-5555         | (1) 135-4892         | 
| OCEAN | Oc?ano Atl?ntico Ltda. | Yvonne Moncada       | Sales Agent          | Ing. Gustavo Moncada 8585 Piso 20-A | Buenos Aires    | (null)          | 1010       | Argentina       | (1) 135-5333         | (1) 135-5535         | 
| RANCH | Rancho grande        | Sergio Guti?rrez     | Sales Representative | Av. del Libertador 900 | Buenos Aires    | (null)          | 1010       | Argentina       | (1) 123-5555         | (1) 123-5556         | 
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

3) Importar un archivo .parquet que contenga toda la tabla orders. Luego ingestar el
archivo a HDFS (carpeta /sqoop/ingest)

sqoop import \
--connect jdbc:postgresql://172.17.0.3:5431/northwind \
--username postgres \
--table region \
--m 1 \
--P \
--target-dir /sqoop/ingest \
--as-parquetfile \
--delete-target-dir

# result

2024-05-21 21:24:04,186 INFO mapreduce.ImportJobBase: Transferred 1.8359 KB in 28.8142 seconds (65.2457 bytes/sec)
2024-05-21 21:24:04,189 INFO mapreduce.ImportJobBase: Retrieved 4 records.
hadoop@d4437f61daec:/$ hdfs dfs -ls /sqoop/ingest/
Found 3 items
drwxr-xr-x   - hadoop supergroup          0 2024-05-21 21:23 /sqoop/ingest/.metadata
drwxr-xr-x   - hadoop supergroup          0 2024-05-21 21:24 /sqoop/ingest/.signals
-rw-r--r--   1 hadoop supergroup        747 2024-05-21 21:24 /sqoop/ingest/9c2891fc-c009-4a4c-942d-87a786423ab8.parquet
hadoop@d4437f61daec:/$ 

4) Importar un archivo .parquet que contenga solo los productos con mas 20 unidades en
stock, de la tabla Products . Luego ingestar el archivo a HDFS (carpeta ingest)

sqoop import \
--connect jdbc:postgresql://172.17.0.3:5431/northwind \
--username postgres \
--table products \
--m 1 \
--P \
--target-dir /sqoop/ingest \
--as-parquetfile \
--where "units_in_stock > 20" \
--delete-target-dir

# result

2024-05-21 21:30:50,927 INFO mapreduce.ImportJobBase: Transferred 8.2568 KB in 21.5123 seconds (393.0305 bytes/sec)
2024-05-21 21:30:50,956 INFO mapreduce.ImportJobBase: Retrieved 48 records.
hadoop@d4437f61daec:/$ hdfs dfs -ls /sqoop/ingest/
Found 3 items
drwxr-xr-x   - hadoop supergroup          0 2024-05-21 21:30 /sqoop/ingest/.metadata
drwxr-xr-x   - hadoop supergroup          0 2024-05-21 21:30 /sqoop/ingest/.signals
-rw-r--r--   1 hadoop supergroup       4974 2024-05-21 21:30 /sqoop/ingest/9e129ac9-f3d2-47d2-94b1-4f32e6285d26.parquet
hadoop@d4437f61daec:/$ nano /sqoop/ingest/9e129ac9-f3d2-47d2-94b1-4f32e6285d26.parquet
hadoop@d4437f61daec:/$ 
