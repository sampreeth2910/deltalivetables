import dlt

#ingesting the sales data

@dlt.table(
    name = 'sales_bronze'
)
def sales_bronze():
    df = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format", "csv")\
                .load("/Volumes/mycatalog/gold_layer/volumes_project/sales/")
    return df
@dlt.table(name = 'customer_bronze')
def customer_bronze():
    df = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format", "csv")\
                .load("/Volumes/mycatalog/gold_layer/volumes_project/customers/")
    return df

@dlt.table(name = 'store_bronze')
def store_bronze():
    df = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format", "csv")\
                .load("/Volumes/mycatalog/gold_layer/volumes_project/stores/")
    return df

@dlt.table(name = 'product_bronze')
def product_bronze():
    df = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format", "csv")\
                .load("/Volumes/mycatalog/gold_layer/volumes_project/products/")
    return df

                 
