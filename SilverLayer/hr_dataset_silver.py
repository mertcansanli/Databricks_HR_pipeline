import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =========================
# EMPLOYEES
# =========================

@dlt.view(name="trans_employees")
def trans_employees():
    return (
        spark.readStream.format("delta")
        .load("/Volumes/workspace/bronze/bronzevolume/hr_dataset/data/")
        .withColumn("Age", col("age").cast(IntegerType()))
        .withColumn("Service_Years", col("service_years").cast(IntegerType()))
        .withColumn("Salary", col("salary").cast(DoubleType()))
        .withColumn("modifiedDate", current_timestamp())
        .drop("_rescued_data")
    )


dlt.create_streaming_table("silver_employees")

dlt.create_auto_cdc_flow(
    target="silver_employees",
    source="trans_employees",
    keys=["employee_id"],
    sequence_by=("modifiedDate"),
    stored_as_scd_type="2"
)

# =========================
# TOTALS
# =========================

@dlt.table(name="stage_totals")
def stage_totals():
    return (
        spark.readStream.format("delta")
        .load("/Volumes/workspace/bronze/bronzevolume/fake_totals/data/")
    )


@dlt.view(name="trans_totals")
def trans_totals():
    return (
        dlt.read_stream("stage_totals")
        .withColumn("Actual", col("Actual").cast(IntegerType()))
        .withColumn("Target", col("Target").cast(IntegerType()))
        .withColumn("modifiedDate", current_timestamp())
        .drop("_rescued_data")
    )


rules = {
    "quarter_not_null": "`Year-Quarter` IS NOT NULL"
}


@dlt.table(name="silver_totals")
@dlt.expect_all_or_drop(rules)
def silver_totals():
    return dlt.read_stream("trans_totals")


# =========================
# WAFFLE GRID
# =========================

@dlt.table(name="stage_waffle")
def stage_waffle():
    return (
        spark.readStream.format("delta")
        .load("/Volumes/workspace/bronze/bronzevolume/fake_waffle_chart/data/")
    )


@dlt.view(name="trans_waffle")
def trans_waffle():
    return (
        dlt.read_stream("stage_waffle")
        .withColumn("Columns", col("Columns").cast(IntegerType()))
        .withColumn("Rows", col("Rows").cast(IntegerType()))
        .withColumn("modifiedDate", current_timestamp())
        .drop("_rescued_data")
    )


rules_waffle = {
    "columns_not_null": "Columns IS NOT NULL",
    "rows_not_null": "Rows IS NOT NULL"
}


@dlt.table(name="silver_waffle")
@dlt.expect_all_or_drop(rules_waffle)
def silver_waffle():
    return dlt.read_stream("trans_waffle")