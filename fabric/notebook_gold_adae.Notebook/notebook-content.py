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

# MAGIC %%configure -f
# MAGIC {"defaultLakehouse": {"name": "lh_clinical"}}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

ae   = spark.table("silver.ae")
adsl = spark.table("gold.adsl").select("USUBJID", "TRT01A", "TRT01P", "SAFFL", "TRTSDT", "TRTEDT")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# AESTDTC can be partial (YYYY or YYYY-MM). Fill missing month/day with 01 so the date isn't lost.
def impute_start(col):
    c = F.col(col)
    return (F.when(F.length(c) >= 10, F.to_date(F.substring(c, 1, 10)))
             .when(F.length(c) == 7, F.to_date(F.concat(c, F.lit("-01"))))
             .when(F.length(c) == 4, F.to_date(F.concat(c, F.lit("-01-01")))))

ae_dt = (
    ae.withColumn("ASTDT", impute_start("AESTDTC"))
      .withColumn("AENDT", F.when(F.length("AEENDTC") >= 10, F.to_date(F.substring("AEENDTC", 1, 10))))
      .withColumn("ASTDTF", F.when(F.length("AESTDTC") == 7, "D").when(F.length("AESTDTC") == 4, "M"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adae = (
    ae_dt.join(adsl, "USUBJID", "inner")      # every AE subject should exist in ADSL
         .withColumn("ASTDY", F.datediff("ASTDT", "TRTSDT") + 1)
         .withColumn("TRTEMFL",
             F.when(F.col("TRTSDT").isNull(), None)
              .when(F.col("ASTDT").isNull() | (F.col("ASTDT") >= F.col("TRTSDT")), "Y")
              .otherwise("N"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert adae.count() == ae.count(), "AE rows lost in ADSL join - check USUBJID mismatch"

# Subjects with at least one TEAE, by arm (count distinct subjects, not AE rows)
(adae.filter("SAFFL = 'Y' AND TRTEMFL = 'Y'")
     .groupBy("TRT01A").agg(F.countDistinct("USUBJID").alias("subj_with_teae"),
                            F.count("*").alias("teae_records"))
     .orderBy("TRT01A").show())

adae.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.adae")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- ADAE validation ---
# 1. Every AE row has a TEAE flag (Y or N, no nulls for dosed subjects)
adae.groupBy("TRTEMFL").count().show()
assert adae.filter("SAFFL = 'Y' AND TRTEMFL IS NULL").count() == 0, "Dosed subjects with unflagged AEs"

# 2. Partial start dates that were filled in (D = day filled, M = month and day filled)
adae.groupBy("ASTDTF").count().show()

# 3. No filled-in date falls in or after the first-dose month while flagged pre-treatment
n_edge = adae.filter(
    "ASTDTF IS NOT NULL AND TRTEMFL = 'N' "
    "AND date_format(ASTDT,'yyyy-MM') >= date_format(TRTSDT,'yyyy-MM')"
).count()
assert n_edge == 0, f"{n_edge} imputed AE dates need same-month TRTSDT rule"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
