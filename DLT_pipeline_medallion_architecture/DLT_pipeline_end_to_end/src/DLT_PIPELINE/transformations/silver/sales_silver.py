import dlt 
from pyspark.sql.functions import * 

@dlt.view(
    name = 'sales_silver_view'
)
def sales_view():
    df_sales=spark.readStream.table('smapreetharigala.sam_db.sales_stg')
    df_sales = df_sales.withColumn("pricePerSale",round(col('total_amount')/col('quantity'),2))
    df_sales= df_sales.withColumn("processDate",current_timestamp())
    return df_sales


dlt.create_streaming_table(name = 'smapreetharigala.sam_db.sales_silver')

dlt.create_auto_cdc_flow(
    target= 'smapreetharigala.sam_db.sales_silver',
    source= 'sales_silver_view',
    keys= ['sales_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type=1
)