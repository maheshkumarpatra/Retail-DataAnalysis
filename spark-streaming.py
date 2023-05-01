# Import important libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('KafkaStream').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Reading input data from Kafka
raw_stream_data = spark.readStream \
                       .format('kafka') \
                       .option('kafka.bootstrap.servers', '18.211.252.152:9092') \
                       .option('subscribe', 'real-time-project') \
                       .option('startingOffsets', 'earliest') \
                       .option('failOnDataLoss','false') \
                       .load()

#Define schema
schema = StructType([
                StructField('invoice_no',LongType(),True),
                StructField('country',StringType(),True),
                StructField('timestamp',TimestampType(),True),
                StructField('type',StringType(),True),
                StructField('items',ArrayType(StructType([
                                StructField("SKU", StringType(),True),
                                StructField("title", StringType(),True),
                                StructField("unit_price", FloatType(),True),
                                StructField("quantity", IntegerType(),True)
                                ])))])


#Reading the Json data in kafka
order_data = raw_stream_data.select(from_json(col("value").cast(
    "string"), schema).alias("data")).select("data.*")

# Calculating the additional columns
# Create Utility method for calculate total_cost
def get_total_cost(items, trns_type):
    if items is not None:
        total_cost = 0
        item_price = 0
        for item in items:
            item_price = (item['quantity'] * item['unit_price'])
        total_cost = total_cost + item_price
        item_price = 0

        if trns_type == "RETURN":
            return total_cost * -1
        else:
            return total_cost

# Create Utility method for calculate total_items
def get_total_items(items):
    if items is not None:
        total_items = 0
        for item in items:
            total_items = total_items + item['quantity']
        return total_items

# Create Utility method for denotes whether an order is a new order or not
def flag_is_order(trns_type):
    if trns_type == 'ORDER':
        return (1)
    else:
        return (0)

# Create Utility method for denotes whether an order is a return order or not
def flag_is_return(trns_type):
    if trns_type == 'RETURN':
        return (1)
    else:
        return (0)

# Converting functions to UDF
TotalCostUDF = udf(get_total_cost, FloatType())
TotalItemsUDF = udf(get_total_items, IntegerType())
IsOrderUDF = udf(flag_is_order, IntegerType())
IsReturnUDF = udf(flag_is_return, IntegerType())

# Add new columns using UDF
new_order_data = order_data \
                .withColumn('total_cost',TotalCostUDF(order_data.items, order_data.type)) \
                .withColumn('total_items', TotalItemsUDF(order_data.items)) \
                .withColumn('is_order',IsOrderUDF(order_data.type)) \
                .withColumn('is_return', IsReturnUDF(order_data.type))

# write the final summarised input values to the console
new_order_data_query = new_order_data \
                    .select("invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order", "is_return") \
                    .writeStream \
                    .outputMode('append') \
                    .format('console') \
                    .option('truncate', 'false') \
                    .trigger(processingTime = '1 minute') \
                    .start()

# calculate time-based KPIs
time_based = new_order_data \
            .withWatermark('timestamp', '1 minute') \
            .groupBy(window('timestamp', '1 minute', '1 minute')) \
            .agg(count("invoice_no").alias("OPM"),
                 sum("total_cost").alias("total_sale_volume"),
                 avg("total_cost").alias("average_transaction_size"),
                 avg("is_return").alias("rate_of_return") ) \
    .select("window", "OPM", "total_sale_volume", "average_transaction_size", "rate_of_return")

# Write time_based KPIs data to HDFS
time_based_query = time_based.writeStream \
   .format("json") \
   .outputMode("append") \
   .option("truncate", "false") \
   .option("path", "time_kpi") \
   .option("checkpointLocation", "time_kpi_checkpoints") \
   .trigger(processingTime="1 minute") \
   .start()

# calculate time-and-country-based KPIs
time_country_based = new_order_data \
                .withWatermark("timestamp", "1 minute") \
                .groupBy(window("timestamp", "1 minute", "1 minute"), "country") \
                .agg(count("invoice_no").alias("OPM"),
                     sum("total_cost").alias("total_sale_volume"),
                     avg("is_return").alias("rate_of_return")) \
    .select("window", "country", "OPM", "total_sale_volume", "rate_of_return")

# Write time-and-country-based KPIs data to HDFS
time_country_based_query = time_country_based.writeStream \
   .format("json") \
   .outputMode("append") \
   .option("truncate", "false") \
   .option("path", "country_kpi") \
   .option("checkpointLocation", "country_kpi_checkpoints") \
   .trigger(processingTime="1 minute") \
   .start()
   

new_order_data_query.awaitTermination()
time_based_query.awaitTermination()
time_country_based_query.awaitTermination()







