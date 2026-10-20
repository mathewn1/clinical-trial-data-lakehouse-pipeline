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

import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import Window

RUN_ID = str(uuid.uuid4())
RUN_TS = datetime.now(timezone.utc)

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

DOMAINS = {
    "dm": ["USUBJID"],
    "ae": ["USUBJID", "AESEQ"],
    "ex": ["USUBJID", "EXSEQ"],
    "ds": ["USUBJID", "DSSEQ"],
    "lb": ["USUBJID", "LBSEQ"],
    "vs": ["USUBJID", "VSSEQ"],
}
print("run_id:", RUN_ID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import xml.etree.ElementTree as ET

# ---- Load define.xml ----
DEFINE_PATH = "/lakehouse/default/Files/raw/sdtm/define.xml"
root = ET.parse(DEFINE_PATH).getroot()
els = list(root.iter())

def local(tag):
    return tag.rsplit("}", 1)[-1]

def attr(el, name):
    for k, v in el.attrib.items():
        if local(k) == name:
            return v
    return None

# ---- Codelist ID -> allowed values ----
codelists = {}
for cl in els:
    if local(cl.tag) == "CodeList":
        codelists[attr(cl, "OID")] = [attr(i, "CodedValue") for i in cl if local(i.tag) == "CodeListItem"]

# ---- Column ID -> (column name, codelist ID) ----
itemdefs = {}
for it in els:
    if local(it.tag) == "ItemDef":
        ref = next((c for c in it if local(c.tag) == "CodeListRef"), None)
        itemdefs[attr(it, "OID")] = (attr(it, "Name"), attr(ref, "CodeListOID") if ref is not None else None)

# ---- Dataset -> its columns ----
domain_ct = {}
required  = {}
for ig in els:
    if local(ig.tag) == "ItemGroupDef":
        domain = attr(ig, "Name").lower()
        domain_ct[domain], required[domain] = {}, []
        for ref in ig:
            if local(ref.tag) != "ItemRef":
                continue
            name, cl_oid = itemdefs.get(attr(ref, "ItemOID"), (None, None))
            if codelists.get(cl_oid):
                domain_ct[domain][name] = codelists[cl_oid]
            if attr(ref, "Mandatory") == "Yes":
                required[domain].append(name)

print(domain_ct["ae"])
print(required["ae"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_strings(df):
    # Trim spaces; blank -> null. Skip lineage columns (start with _).
    for col, dtype in df.dtypes:
        if dtype == "string" and not col.startswith("_"):
            df = df.withColumn(col, F.when(F.trim(F.col(col)) == "", None)
                                     .otherwise(F.trim(F.col(col))))
    return df

def cast_numbers(df):
    # SEQ/DY/AGE -> int, MedDRA codes (CD) -> long,
    # VISITNUM -> rounded to 2 decimals (removes float noise like 1.2000000000000002)
    for col, dtype in df.dtypes:
        if dtype != "double":
            continue
        if col.endswith("SEQ") or col.endswith("DY") or col == "AGE":
            df = df.withColumn(col, F.col(col).cast("int"))
        elif col.endswith("CD"):
            df = df.withColumn(col, F.col(col).cast("long"))
        elif col == "VISITNUM":
            df = df.withColumn(col, F.round(F.col(col), 2))
    return df

def standardise_ct(df, ct):
    # Match values to the define.xml codelist, ignoring case.
    # Text columns only: numeric codes like VISITNUM keep their type.
    types = dict(df.dtypes)
    for col, allowed in ct.items():
        if types.get(col) != "string":
            continue
        lookup = F.create_map(*[F.lit(x) for v in allowed for x in (v.upper(), v)])
        df = df.withColumn(col, F.coalesce(lookup[F.upper(F.col(col))], F.col(col)))
    return df

def parse_dtcs(df):
    # Each --DTC gets _PREC, _DT, _TS. Original string kept.
    for col in [c for c in df.columns if c.endswith("DTC")]:
        v = F.col(col)
        prec = (F.when(v.rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}"), "datetime")
                 .when(v.rlike(r"^\d{4}-\d{2}-\d{2}$"), "date")
                 .when(v.rlike(r"^\d{4}-\d{2}$"), "month")
                 .when(v.rlike(r"^\d{4}$"), "year"))
        df = (df.withColumn(f"{col}_PREC", prec)
                .withColumn(f"{col}_DT", F.when(prec.isin("date", "datetime"),
                                                F.to_date(F.substring(v, 1, 10), "yyyy-MM-dd")))
                .withColumn(f"{col}_TS", F.when(prec == "datetime",
                                                F.to_timestamp(F.substring(v, 1, 16), "yyyy-MM-dd'T'HH:mm"))))
    return df

print("helpers ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def to_silver(domain, keys):
    df = spark.table(f"bronze.{domain}")
    df = clean_strings(df)
    df = cast_numbers(df)
    df = standardise_ct(df, domain_ct.get(domain, {}))
    df = parse_dtcs(df)

    n_before = df.count()
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    dups = n_before - df.count()

    df = (df.withColumn("_run_id", F.lit(RUN_ID))
            .withColumn("_silver_loaded_at", F.lit(RUN_TS)))
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"silver.{domain}")

    out = spark.table(f"silver.{domain}")
    print(f"silver.{domain}: {out.count():,} rows ({dups} duplicates dropped)")
    return out, dups

# Run it for every domain in DOMAINS (cell 1)
silver = {d: to_silver(d, keys) for d, keys in DOMAINS.items()}
dm, dm_dups = silver["dm"]
ae, ae_dups = silver["ae"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = []

def check(table, name, bad_rows, severity="error"):
    n = bad_rows if isinstance(bad_rows, int) else bad_rows.count()
    results.append((RUN_ID, RUN_TS, table, name, severity, n, "PASS" if n == 0 else "FAIL"))

def standard_checks(table, df, keys, dups, extra_required=()):
    check(table, f"duplicate key {keys} in bronze", dups, "warn")

    for col in dict.fromkeys(list(required.get(table, [])) + list(extra_required)):
        if col in df.columns:
            check(table, f"{col} is null", df.filter(F.col(col).isNull()))

    for col in [c for c in df.columns if c.endswith("DTC")]:
        bad = df.filter(F.col(col).isNotNull() & (
            F.col(f"{col}_PREC").isNull() |
            (F.col(f"{col}_PREC").isin("date", "datetime") & F.col(f"{col}_DT").isNull())))
        check(table, f"{col} not valid ISO 8601", bad)

    types = dict(df.dtypes)
    for col, allowed in domain_ct.get(table, {}).items():
        if col not in df.columns:
            continue
        if types[col] == "string":
            values, c = allowed, F.col(col)
        else:  # numeric column: compare as numbers, not text
            values, c = [float(v) for v in allowed], F.col(col).cast("double")
        check(table, f"{col} outside codelist",
              df.filter(F.col(col).isNotNull() & ~c.isin(values)), "warn")

# --- DM ---
standard_checks("dm", dm, DOMAINS["dm"], dm_dups)
check("dm", "AGE outside 0-120", dm.filter((F.col("AGE") < 0) | (F.col("AGE") > 120)))

# --- AE ---
standard_checks("ae", ae, DOMAINS["ae"], ae_dups, extra_required=["AEDECOD"])
check("ae", "USUBJID not in DM", ae.join(dm.select("USUBJID"), "USUBJID", "left_anti"))
check("ae", "AEENDTC before AESTDTC", ae.filter(F.col("AEENDTC_DT") < F.col("AESTDTC_DT")))
check("ae", "AESTDTC partial (month/year)", ae.filter(F.col("AESTDTC_PREC").isin("month", "year")), "warn")
check("ae", "same event recorded more than once (USUBJID, AESPID, AESTDTC)",
      ae.groupBy("USUBJID", "AESPID", "AESTDTC").count().filter("count > 1"), "warn")
check("ae", "end date present but outcome NOT RECOVERED",
      ae.filter(F.col("AEENDTC").isNotNull() & (F.col("AEOUT") == "NOT RECOVERED/NOT RESOLVED")), "warn")

# --- EX, DS ---
standard_checks("ex", silver["ex"][0], DOMAINS["ex"], silver["ex"][1])
standard_checks("ds", silver["ds"][0], DOMAINS["ds"], silver["ds"][1])

# --- LB, VS ---
standard_checks("lb", silver["lb"][0], DOMAINS["lb"], silver["lb"][1])
standard_checks("vs", silver["vs"][0], DOMAINS["vs"], silver["vs"][1])

print(len(results), "checks run")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dq = spark.createDataFrame(results,
    "run_id string, run_ts timestamp, table_name string, check_name string, "
    "severity string, failed_rows long, status string")

dq.write.mode("append").saveAsTable("silver.dq_results")
display(dq.filter("status = 'FAIL'"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
  SELECT run_id, MIN(run_ts) AS run_ts, COUNT(*) AS checks,
         SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS fails
  FROM silver.dq_results
  GROUP BY run_id ORDER BY run_ts
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
  SELECT table_name, check_name, severity, failed_rows
  FROM silver.dq_results
  WHERE run_id = (SELECT run_id FROM silver.dq_results ORDER BY run_ts DESC LIMIT 1)
    AND status = 'FAIL'
  ORDER BY severity, table_name
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("codelist:", domain_ct["ex"]["VISITNUM"])
spark.table("silver.ex").select("VISITNUM").distinct().orderBy("VISITNUM").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
