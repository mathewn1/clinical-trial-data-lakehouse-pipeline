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

from pyspark.sql import functions as F, Window as W

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

dm = spark.table("silver.dm")
ex = spark.table("silver.ex")
ds = spark.table("silver.ds")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# First and last dose date for each patient. If the end date is missing, use the start date.
ex_dates = (
    ex.withColumn("st", F.to_date(F.substring("EXSTDTC", 1, 10)))
      .withColumn("en", F.to_date(F.substring(F.coalesce("EXENDTC", "EXSTDTC"), 1, 10)))
      .groupBy("USUBJID")
      .agg(F.min("st").alias("TRTSDT"), F.max("en").alias("TRTEDT"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Keep only DSCAT = DISPOSITION EVENT. Rows like RANDOMIZED are milestones, not disposition.
ds_disp = (
    ds.filter(F.col("DSCAT") == "DISPOSITION EVENT")
      .withColumn("rn", F.row_number().over(
          W.partitionBy("USUBJID").orderBy(F.col("DSSTDTC").desc())))
      .filter("rn = 1")
      .select(
          "USUBJID",
          F.when(F.col("DSDECOD") == "COMPLETED", "COMPLETED")
           .otherwise("DISCONTINUED").alias("EOSSTT"),
          F.when(F.col("DSDECOD") != "COMPLETED", F.col("DSDECOD")).alias("DCSREAS"),
      )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adsl = (
    dm.select("STUDYID", "USUBJID", "SUBJID", "SITEID", "AGE", "SEX", "RACE",
              "ARM", "ARMCD", "ACTARM")
      .join(ex_dates, "USUBJID", "left")
      .join(ds_disp, "USUBJID", "left")
      .withColumn("TRT01P", F.col("ARM"))
      .withColumn("TRT01A", F.col("ARM"))          # aligned to CDISC Pilot reference ADSL
      .withColumn("ACTARM_DM", F.col("ACTARM"))    # kept for traceability (12 High→Low)
      .withColumn("AGEGR1", F.when(F.col("AGE") < 65, "<65")
                             .when(F.col("AGE") <= 80, "65-80")
                             .otherwise(">80"))
      .withColumn("TRTDURD", F.datediff("TRTEDT", "TRTSDT") + 1)
      .withColumn("SAFFL", F.when(F.col("TRTSDT").isNotNull(), "Y").otherwise("N"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

n_rows, n_subj = adsl.count(), adsl.select("USUBJID").distinct().count()
assert n_rows == n_subj, f"ADSL not one row per subject: {n_rows} rows vs {n_subj}"
assert n_rows == dm.count(), "Lost or gained subjects vs DM"

adsl.groupBy("TRT01A", "SAFFL").count().orderBy("TRT01A").show()

adsl.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.adsl")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adsl.filter("SAFFL = 'Y'").groupBy("TRT01P", "TRT01A").count().orderBy("TRT01P").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
