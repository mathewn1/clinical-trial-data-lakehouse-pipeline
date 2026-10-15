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

ae = spark.table("silver.ae")

dupe_keys = (ae.groupBy("USUBJID", "AETERM", "AESTDTC").count().filter("count > 1"))
print("count of 2 vs 3+:"); dupe_keys.groupBy("count").count().show()

# Look at a few in full
sample = dupe_keys.limit(3).join(ae, ["USUBJID", "AETERM", "AESTDTC"])
display(sample.select("USUBJID", "AESEQ", "AETERM", "AESTDTC", "AEENDTC",
                      "AESEV", "AESER", "AEOUT", "AESPID").orderBy("USUBJID", "AETERM", "AESEQ"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
