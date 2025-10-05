import dlt 
from pyspark.sql.functions import *

@dlt.view (name = 'sales_silver_view')
def sales_silver_view():
    df = spark.readStream.table('sales_bronze')
    df = df.withColumn("pricePerSale",round(col("total_amount")/col("quantity"),2))
    df = df.withColumn("processDate",current_timestamp())
    return df

# SALES SILVER TABLE (WITH UPSERT)
dlt.create_streaming_table(name = 'sales_silver')
dlt.create_auto_cdc_flow(
    target = 'sales_silver',
    source = 'sales_silver_view',
    keys = ['sales_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type=1
)