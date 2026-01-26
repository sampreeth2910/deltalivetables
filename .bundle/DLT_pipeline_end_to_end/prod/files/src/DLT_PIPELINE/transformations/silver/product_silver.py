import dlt 
from pyspark.sql.functions import * 

@dlt.view(
    name = 'product_silver_view'
)
def sales_view():
    df_prd=spark.readStream.table('smapreetharigala.sam_db.product_stg')
    df_prd =df_prd.withColumn("processDate",current_timestamp())
    return df_prd


dlt.create_streaming_table(name = 'smapreetharigala.sam_db.product_silver')

dlt.create_auto_cdc_flow(
    target= 'smapreetharigala.sam_db.product_silver',
    source= 'product_silver_view',
    keys= ['product_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type=1
)