import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame


def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    selected = dfc.select(list(dfc.keys())[0]).toDF()
    selected.createOrReplaceTempView("non_aggregated_purchase_details")
    totals = spark.sql("""
        select
            sum(total_price) as total_price, payment_state, customer_id, first_name, last_name, city
        from non_aggregated_purchase_details
        group by customer_id, first_name, last_name, city, payment_state
    """)
    results = DynamicFrame.fromDF(totals, glueContext, "results")
    return DynamicFrameCollection({"results": results}, glueContext)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "db_retail", table_name = "src_orders", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "db_retail", table_name = "src_orders", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("col0", "long", "order_id", "long"), ("col1", "string", "order_datetime", "string"), ("col2", "long", "orders__customer_id", "long"), ("col3", "string", "payment_state", "string")], transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [frame = DataSource0]
Transform2 = ApplyMapping.apply(frame = DataSource0, mappings = [("col0", "long", "order_id", "long"), ("col1", "string", "order_datetime", "string"), ("col2", "long", "orders__customer_id", "long"), ("col3", "string", "payment_state", "string")], transformation_ctx = "Transform2")
## @type: DataSource
## @args: [database = "db_retail", table_name = "src_customers", transformation_ctx = "DataSource2"]
## @return: DataSource2
## @inputs: []
DataSource2 = glueContext.create_dynamic_frame.from_catalog(database = "db_retail", table_name = "src_customers", transformation_ctx = "DataSource2")
## @type: ApplyMapping
## @args: [mappings = [("col0", "long", "customer_id", "long"), ("col1", "string", "first_name", "string"), ("col2", "string", "last_name", "string"), ("col6", "string", "city", "string")], transformation_ctx = "Transform7"]
## @return: Transform7
## @inputs: [frame = DataSource2]
Transform7 = ApplyMapping.apply(frame = DataSource2, mappings = [("col0", "long", "customer_id", "long"), ("col1", "string", "first_name", "string"), ("col2", "string", "last_name", "string"), ("col6", "string", "city", "string")], transformation_ctx = "Transform7")
## @type: DataSource
## @args: [format_options = {"quoteChar":"\"","escaper":"","withHeader":False,"separator":","}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://glue-handson-data/Retail-Data-Analysis/SRC/order_items/"], "recurse":True}, transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_options(format_options = {"quoteChar":"\"","escaper":"","withHeader":False,"separator":","}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://glue-handson-data/Retail-Data-Analysis/SRC/order_items/"], "recurse":True}, transformation_ctx = "DataSource1")
## @type: ApplyMapping
## @args: [mappings = [("col0", "long", "order_item_id", "long"), ("col1", "long", "order_items__order_id", "long"), ("col4", "double", "total_price", "double")], transformation_ctx = "Transform6"]
## @return: Transform6
## @inputs: [frame = DataSource1]
Transform6 = ApplyMapping.apply(frame = DataSource1, mappings = [("col0", "long", "order_item_id", "long"), ("col1", "long", "order_items__order_id", "long"), ("col4", "double", "total_price", "double")], transformation_ctx = "Transform6")
## @type: Join
## @args: [keys2 = ["order_id"], keys1 = ["order_items__order_id"], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame1 = Transform6, frame2 = Transform2]
Transform0 = Join.apply(frame1 = Transform6, frame2 = Transform2, keys2 = ["order_id"], keys1 = ["order_items__order_id"], transformation_ctx = "Transform0")
## @type: ApplyMapping
## @args: [mappings = [("total_price", "double", "total_price", "double"), ("orders__customer_id", "long", "orders__customer_id", "long"), ("payment_state", "string", "payment_state", "string")], transformation_ctx = "Transform4"]
## @return: Transform4
## @inputs: [frame = Transform0]
Transform4 = ApplyMapping.apply(frame = Transform0, mappings = [("total_price", "double", "total_price", "double"), ("orders__customer_id", "long", "orders__customer_id", "long"), ("payment_state", "string", "payment_state", "string")], transformation_ctx = "Transform4")
## @type: Join
## @args: [keys2 = ["customer_id"], keys1 = ["orders__customer_id"], transformation_ctx = "Transform3"]
## @return: Transform3
## @inputs: [frame1 = Transform4, frame2 = Transform7]
Transform3 = Join.apply(frame1 = Transform4, frame2 = Transform7, keys2 = ["customer_id"], keys1 = ["orders__customer_id"], transformation_ctx = "Transform3")
## @type: ApplyMapping
## @args: [mappings = [("total_price", "double", "total_price", "double"), ("payment_state", "string", "payment_state", "string"), ("customer_id", "long", "customer_id", "long"), ("first_name", "string", "first_name", "string"), ("last_name", "string", "last_name", "string"), ("city", "string", "city", "string")], transformation_ctx = "Transform10"]
## @return: Transform10
## @inputs: [frame = Transform3]
# Transform10 = ApplyMapping.apply(frame = Transform3, mappings = [("total_price", "double", "total_price", "double"), ("payment_state", "string", "payment_state", "string"), ("customer_id", "long", "customer_id", "long"), ("first_name", "string", "first_name", "string"), ("last_name", "string", "last_name", "string"), ("city", "string", "city", "string")], transformation_ctx = "Transform10")
Transform10_df = Transform3.toDF().select("total_price", "payment_state", "customer_id", "first_name", "last_name", "city")
Transform10 = DynamicFrame.fromDF(Transform10_df, glueContext, "Transform10")
## @type: Filter
## @args: [f = lambda row : (row["customer_id"] == 11599), transformation_ctx = "Transform9"]
## @return: Transform9
## @inputs: [frame = Transform10]
Transform9 = Filter.apply(frame = Transform10, f = lambda row : (row["customer_id"] == 7029), transformation_ctx = "Transform9")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"Transform9": Transform9}, glueContext), className = MyTransform, transformation_ctx = "Transform8"]
## @return: Transform8
## @inputs: [dfc = Transform9]
Transform8 = MyTransform(glueContext, DynamicFrameCollection({"Transform10": Transform10}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform8.keys())[0], transformation_ctx = "Transform5"]
## @return: Transform5
## @inputs: [dfc = Transform8]
Transform5 = SelectFromCollection.apply(dfc = Transform8, key = list(Transform8.keys())[0], transformation_ctx = "Transform5")
## @type: ApplyMapping
## @args: [mappings = [("total_price", "double", "payment_amount", "double"), ("payment_state", "string", "payment_state", "string"), ("customer_id", "long", "customer_id", "long"), ("first_name", "string", "first_name", "string"), ("last_name", "string", "last_name", "string"), ("city", "string", "city", "string")], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [frame = Transform5]
Transform1 = ApplyMapping.apply(frame = Transform5, mappings = [("total_price", "double", "payment_amount", "double"), ("payment_state", "string", "payment_state", "string"), ("customer_id", "long", "customer_id", "long"), ("first_name", "string", "first_name", "string"), ("last_name", "string", "last_name", "string"), ("city", "string", "city", "string")], transformation_ctx = "Transform1")
## @type: DataSink
## @args: [connection_type = "s3", format = "csv", connection_options = {"path": "s3://glue-handson-data/Retail-Data-Analysis/TGT/payment_details/", "partitionKeys": ["payment_state"]}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform1]
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform1, connection_type = "s3", format = "csv", connection_options = {"path": "s3://glue-handson-data/Retail-Data-Analysis/TGT/payment_details/", "partitionKeys": ["payment_state"]}, transformation_ctx = "DataSink0")
job.commit()