import dlt 
from pyspark.sql.functions import *

@dlt.view(name= 'dim_products_view')
def dim_stores_view():
  return (spark.readStream.table('product_silver_view'))

dlt.create_streaming_table(name = 'dim_products')
dlt.create_auto_cdc_flow(
    target='dim_products',
    source = 'dim_products_view',
    keys=['product_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type=2,
    except_column_list=['processDate']
)