# SCD_1 in Databricks Notebook
from delta.tables import DeltaTable
from pyspark.sql.functions import *

# Create Data frame

data = [(1,"John","HYD","IT"),(2,"Roshan","HYD","HR")]
schema=["Id","Name","Location","Dept"]
df=spark.createDataFrame(data,schema)
df.display()

# Save as Delta table

delta_table_path = "dbfs:/mnt/delta/employees"
df.write.format("delta").mode("overwrite").save(delta_table_path)

# New data added into delta table 

new_data =[(1, "John", "BLR", "IT"),  # Update Location
            (2, "Roshan", "HYD", "Finance"),  # Update Dept
            (3, "Mike", "Pune", "IT")] 

df_new = spark.createDataFrame(new_data, schema)

#  reads a Delta Lake table in Databricks/Spark and displays its data.

deltatable= DeltaTable.forPath(spark , delta_table_path )
deltatable.toDF().show()

# Delta Lake merge operation is used for upserting data (update existing records and insert new ones).

deltatable.alias("target").merge(
df_new.alias("source"),
"target.Id = source.Id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

deltatable.toDF().show()

# heading type

# MAGIC %md
# MAGIC ## # whenMatchedUpdateAll(condition) → Updates all columns but only when a change is detected.
# MAGIC ## # whenMatchedUpdate(set={}) → Explicitly updates EndDate and IsCurrent when a match is found.
# MAGIC ## # whenNotMatchedInsertAll() → Inserts new records into the Delta table.

# SDC_2

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DateType

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Define schema explicitly
schema = StructType([
    StructField("Id", IntegerType(), False),
    StructField("Name", StringType(), False),
    StructField("Location", StringType(), False),
    StructField("Dept", StringType(), False),
    StructField("EffectiveDate", StringType(), False),  # Change to DateType() if needed
    StructField("EndDate", StringType(), True),  # Can be None, so nullable=True
    StructField("IsCurrent", BooleanType(), False)
])

# Create Sample Existing Data
data1 = [
    (1, "John", "HYD", "IT", "2024-03-01", None, True),
    (2, "Roshan", "HYD", "HR", "2024-03-01", None, True)
]

df1 = spark.createDataFrame(data1, schema=schema)

# Show DataFrame
df1.show()




# COMMAND ----------


from delta.tables import DeltaTable
delta_table_path = "/tmp/delta/history_table"
df1.write.format("delta").mode("overwrite").save(delta_table_path)



# COMMAND ----------

data_new = [
    (1, "John", "BLR", "IT", "2024-03-15", None, True),  # Location changed
    (2, "Roshan", "HYD", "HR", "2024-03-01", None, True),  # No change
    (3, "Alice", "PUNE", "FIN", "2024-03-15", None, True)  # New record
]

# Load the Delta Table

delta_table = DeltaTable.forPath(spark, delta_table_path)
delta_table.toDF().show()

df_new = spark.createDataFrame(data_new, schema=schema)
df_new.show()

# Performing MERGE Operation for SCD Type 1 using Delta Lake

delta_table.alias("old").merge(
    df_new.alias("new"),
    "old.Id = new.Id"
).whenMatchedUpdate(
    condition="old.Location <> new.Location OR old.Dept <> new.Dept OR old.Name <> new.Name",
    set={
        "EndDate": current_date(),  # Closing old records
        "IsCurrent": lit(False)
    }
).whenNotMatchedInsertAll().execute()






# Reading the delta table

df_final = spark.read.format("delta").load(delta_table_path)
df_final.show()


# MAGIC %md 
# MAGIC # SCD_TYPE_2

import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

from delta.tables import DeltaTable

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SCD Type 2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define Delta Table Path
delta_table_path = "/tmp/delta/history_table"

# Define Schema with EffectiveDate as StringType()
schema = StructType([
    StructField("Id", IntegerType(), False),
    StructField("Name", StringType(), False),
    StructField("Location", StringType(), False),
    StructField("Dept", StringType(), False),
    StructField("EffectiveDate", StringType(), False),  # ✅ Store as StringType()
    StructField("EndDate", StringType(), True),  # ✅ Ensure StringType() for consistency
    StructField("IsCurrent", BooleanType(), False)
])

# Function to format date as string (YYYY-MM-DD)
def to_date_str(date_str):
    return datetime.datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d") if date_str else None

# Sample Existing Data (Stored as Strings)
data_existing = [
    (1, "John", "HYD", "IT", to_date_str("2024-03-01"), None, True),
    (2, "Roshan", "HYD", "HR", to_date_str("2024-03-01"), None, True)
]

df_existing = spark.createDataFrame(data_existing, schema=schema)

# Overwrite Delta Table with Initial Data (Only for first-time setup)
df_existing.write.format("delta").mode("overwrite").save(delta_table_path)

# Load Delta Table
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Sample New Incoming Data
data_new = [
    (1, "John", "BLR", "IT", to_date_str("2024-03-15"), None, True),  # Location changed
    (2, "Roshan", "HYD", "HR", to_date_str("2024-03-01"), None, True),  # No change
    (3, "Alice", "PUNE", "FIN", to_date_str("2024-03-15"), None, True)  # New record
]

df_new = spark.createDataFrame(data_new, schema=schema)

# ✅ Ensure `EffectiveDate` is StringType()
df_new = df_new.withColumn("EffectiveDate", col("EffectiveDate").cast(StringType()))
df_new = df_new.withColumn("EndDate", col("EndDate").cast(StringType()))


# Perform MERGE operation (SCD Type 2) 


delta_table.alias("old").merge(
    df_new.alias("new"),
    "old.Id = new.Id AND old.IsCurrent = True"
).whenMatchedUpdate(
    condition="old.Location <> new.Location OR old.Dept <> new.Dept OR old.Name <> new.Name",
    set={
        "EndDate": current_date().cast(StringType()),  # ✅ Convert to StringType()
        "IsCurrent": lit(False)
    }
).whenNotMatchedInsertAll().execute()

# Insert new records (updated versions and brand-new records)
df_new_filtered = df_new.alias("new").join(
    delta_table.toDF().alias("old"),
    on="Id",
    how="left"
).filter(
    (col("old.Id").isNull()) |  # Insert if it's a new record
    (col("old.Location") != col("new.Location")) | 
    (col("old.Dept") != col("new.Dept")) |
    (col("old.Name") != col("new.Name"))
).select(
    col("new.Id"),
    col("new.Name"),
    col("new.Location"),
    col("new.Dept"),
    current_date().cast(StringType()).alias("EffectiveDate"),  # ✅ Convert to StringType()
    lit(None).cast(StringType()).alias("EndDate"),  # ✅ Convert to StringType()
    lit(True).alias("IsCurrent")  # Mark as active
)

df_new_filtered.write.format("delta").mode("append").save(delta_table_path)

# Read final table after merge
df_final = spark.read.format("delta").load(delta_table_path)
df_final.show()


# Readingthe delta table 

spark.read.format("delta").load(delta_table_path).printSchema()

