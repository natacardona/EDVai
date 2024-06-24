from pyspark.sql import HiveContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.functions import round

sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

file_rental_location = 'hdfs://172.17.0.2:9000/ingest/CarRentalData.csv'
file_georef_location = 'hdfs://172.17.0.2:9000/ingest/georef-united-states-of-america-state.csv'

# Leer los archivos CSV especificando el delimitador
df_rental_locations = spark.read.option("delimiter", ",").option("header", "true").csv(file_rental_location)
df_georef_location = spark.read.option("delimiter", ";").option("header", "true").csv(file_georef_location)

# Mostrar las columnas de cada archivo
df_rental_locations.printSchema()
df_georef_location.printSchema()

df_rental_locations = df_rental_locations.withColumnRenamed("fuelType", "fueltype") \
    .withColumnRenamed("renterTripsTaken", "rentertripstaken") \
    .withColumnRenamed("reviewCount", "reviewcount") \
    .withColumnRenamed("location.city", "city") \
    .withColumnRenamed("location.country", "country") \
    .withColumnRenamed("location.latitude", "location_latitude") \
    .withColumnRenamed("location.longitude", "location_longitude") \
    .withColumnRenamed("location.state", "state_name") \
    .withColumnRenamed("owner.id", "owner_id") \
    .withColumnRenamed("rate.daily", "rate_daily") \
    .withColumnRenamed("vehicle.make", "make") \
    .withColumnRenamed("vehicle.model", "model") \
    .withColumnRenamed("vehicle.type", "type") \
    .withColumnRenamed("vehicle.year", "year")
    
df_georef_location = df_georef_location.withColumnRenamed("Geo Point", "geo_point") \
    .withColumnRenamed("Geo Shape", "geo_shape") \
    .withColumnRenamed("Year", "year_georef") \
    .withColumnRenamed("Official Code State", "official_code_state") \
    .withColumnRenamed("Official Name State", "official_name_state") \
    .withColumnRenamed("Iso 3166-3 Area Code", "iso_3166_3_area_code") \
    .withColumnRenamed("Type", "type") \
    .withColumnRenamed("United States Postal Service state abbreviation", "usps_state_abbreviation") \
    .withColumnRenamed("State FIPS Code", "state_fips_code") \
    .withColumnRenamed("State GNIS Code", "state_gnis_code")
    
# Seleccionar y castear las columnas necesarias
df_rental_locations = df_rental_locations.select(
    round(col("rating"), 1).cast("int").alias("rating"),
    col("rentertripstaken").cast("int"),
    col("reviewcount").cast("int"),
    col("rate_daily").cast("int"),
    col("year").cast("int"),
    col("fueltype"),
    col("city"),
    col("country"),
    col("location_latitude"),
    col("location_longitude"),
    col("state_name"),
    col("owner_id"),
    col("make"),
    col("model"),
    col("type")
)

df_inner_joined_rental_location = df_rental_locations.join(df_georef_location, 
                                                     df_rental_locations["state_name"] == df_georef_location["usps_state_abbreviation"], 
                                                     'inner')

# Filtrando rating mayores a cero y adicionalmente para el estados que no sean los de Texas (Filtraremos por state_name)
df_filtered_rating_diff_than_zero = df_inner_joined_rental_location.filter(df_inner_joined_rental_location["rating"] != 0)
df_filtered_texas = df_filtered_rating_diff_than_zero.filter(df_filtered_rating_diff_than_zero["state_name"] != "TX")

# Extraemos las columnas que no son necesarias
df_filtered_texas = df_filtered_texas.drop("country") \
    .drop("location_latitude") \
    .drop("location_longitude") \
    .drop("type") \
    .drop("geo_point") \
    .drop("geo_shape") \
    .drop("official_code_state") \
    .drop("official_name_state") \
    .drop("iso_3166_3_area_code") \
    .drop("usps_state_abbreviation") \
    .drop("state_fips_code") \
    .drop("state_gnis_code") \
    .drop("year_georef")

df_filtered_texas.createOrReplaceTempView("tmp_car_analytics_data")

hc.sql("insert into car_rental_db.car_rental_analytics select * from tmp_car_analytics_data;")