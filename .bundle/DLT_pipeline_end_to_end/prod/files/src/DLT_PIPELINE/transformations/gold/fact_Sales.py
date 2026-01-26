import dlt
from pyspark.sql.functions import *

@dlt.view(name='fact_Sales')
def fact_Sales():
    return (
        spark.readStream.table('sales_silver_view')
    )

# creating the streaming table with auto CDC

dlt.create_streaming_table(name= 'fact_sales_stream')
dlt.create_auto_cdc_flow(
    target = 'fact_sales_stream',
    source = 'fact_Sales',
    keys = ['sales_id'],
    sequence_by= col('processDate'),
    stored_as_scd_type=2
)