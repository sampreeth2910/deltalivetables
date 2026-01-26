import dlt 
from pyspark.sql.functions import *

@dlt.view(name= 'dim_customer_view')
def dim_stores_view():
  return (spark.readStream.table('customer_silver_view'))

dlt.create_streaming_table(name = 'dim_customers')
dlt.create_auto_cdc_flow(
    target='dim_customers',
    source = 'dim_customer_view',
    keys=['customer_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type=2,
    except_column_list=['processDate']
)