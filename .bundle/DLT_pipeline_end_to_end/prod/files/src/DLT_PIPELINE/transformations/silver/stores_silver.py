import dlt 
from pyspark.sql.functions import * 

@dlt.view(
    name = 'stores_silver_view'
)
def sales_view():
    df_str=spark.readStream.table('smapreetharigala.sam_db.store_Stg')
    df_str=df_str.withColumn('store_name',regexp_replace(col('store_name'),'_',''))
    df_str =df_str.withColumn("processDate",current_timestamp())
    return df_str


dlt.create_streaming_table(name = 'smapreetharigala.sam_db.stores_silver')

dlt.create_auto_cdc_flow(
    target= 'smapreetharigala.sam_db.stores_silver',
    source= 'stores_silver_view',
    keys= ['store_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type=1
)