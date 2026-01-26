import dlt 

@dlt.table(name = 'smapreetharigala.sam_db.customer_stg')
def customer_stg():
    return (
        spark.readStream.format('cloudFiles').option('cloudFiles.format','csv')
        .load('/Volumes/smapreetharigala/samschema/dlt_bronze/customers/')
    )

@dlt.table(name = 'smapreetharigala.sam_db.product_stg')
def product_stg():
    return (
        spark.readStream.format('cloudFiles').option('cloudFiles.format','csv')
        .load('/Volumes/smapreetharigala/samschema/dlt_bronze/products/')
    )

@dlt.table(name = 'smapreetharigala.sam_db.sales_stg')
def orders_stg():
    return (
        spark.readStream.format('cloudFiles').option('cloudFiles.format','csv')
        .load('/Volumes/smapreetharigala/samschema/dlt_bronze/sales/')
    )

@dlt.table(name = 'smapreetharigala.sam_db.store_Stg')
def store_Stg():
    return (
        spark.readStream.format('cloudFiles').option('cloudFiles.format','csv')
        .load('/Volumes/smapreetharigala/samschema/dlt_bronze/stores/')
    )