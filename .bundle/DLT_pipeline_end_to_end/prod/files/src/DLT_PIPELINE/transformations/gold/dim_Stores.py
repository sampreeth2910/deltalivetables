import dlt 
from pyspark.sql.functions import *

@dlt.view(name= 'dim_stores_view')
def dim_stores_view():
  return (spark.readStream.table('stores_silver_view'))

dlt.create_streaming_table(name = 'dim_stores')
dlt.create_auto_cdc_flow(
    target='dim_stores',
    source = 'dim_stores_view',
    keys=['store_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type=2,
    except_column_list=['processDate']
)