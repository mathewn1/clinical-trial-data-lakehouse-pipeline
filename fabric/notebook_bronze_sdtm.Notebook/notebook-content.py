# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b414d75b-4eef-40b8-ada7-d2674cfdbf68",
# META       "default_lakehouse_name": "lh_clinical",
# META       "default_lakehouse_workspace_id": "da1e51a5-2e2f-4010-90cc-e3fb6e54f25f",
# META       "known_lakehouses": [
# META         {
# META           "id": "b414d75b-4eef-40b8-ada7-d2674cfdbf68"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import os, glob
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

src = "/lakehouse/default/Files/raw/sdtm"
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

for path in sorted(glob.glob(f"{src}/*.xpt")):
    name = os.path.splitext(os.path.basename(path))[0].lower()
    pdf = pd.read_sas(path, format="xport", encoding="utf-8")

    # numeric -> double, everything else -> string
    schema = StructType([
        StructField(c, DoubleType() if pd.api.types.is_numeric_dtype(pdf[c]) else StringType(), True)
        for c in pdf.columns
    ])
    pdf = pdf.astype(object).where(pdf.notna(), None)

    sdf = (spark.createDataFrame(pdf.values.tolist(), schema)
           .withColumn("_source_file", F.lit(os.path.basename(path)))
           .withColumn("_ingested_at", F.current_timestamp()))

    sdf.write.mode("overwrite").format("delta").saveAsTable(f"bronze.{name}")
    print(f"bronze.{name}: {sdf.count()} rows")

spark.sql("SHOW TABLES IN bronze").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
