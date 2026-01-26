import dlt 
from pyspark.sql.functions import * 

@dlt.view(
    name = 'customer_silver_view'
)
def sales_view():
    df_cust=spark.readStream.table('smapreetharigala.sam_db.customer_stg')
    df_cust = df_cust.withColumn("name",upper(col('name')))
    df_cust =df_cust.withColumn('domain',split(col('email'),'@')[1])
    df_cust =df_cust.withColumn("processDate",current_timestamp())
    df_cust =df_cust.withColumn("processDate",current_timestamp())
    return df_cust


dlt.create_streaming_table(name = 'smapreetharigala.sam_db.customer_silver')

dlt.create_auto_cdc_flow(
    target= 'smapreetharigala.sam_db.customer_silver',
    source= 'customer_silver_view',
    keys= ['customer_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type=1
)