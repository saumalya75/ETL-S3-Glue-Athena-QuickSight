import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="esgaq-src",
    table_name="src_redshift_write_poc",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("year", "long", "year", "long"),
        (
            "industry_aggregation_nzsioc",
            "string",
            "industry_aggregation_nzsioc",
            "string",
        ),
        ("industry_code_nzsioc", "string", "industry_code_nzsioc", "string"),
        ("industry_name_nzsioc", "string", "industry_name_nzsioc", "string"),
        ("units", "string", "units", "string"),
        ("variable_code", "string", "variable_code", "string"),
        ("variable_name", "string", "variable_name", "string"),
        ("variable_category", "string", "variable_category", "string"),
        ("value", "string", "value", "string"),
        ("industry_code_anzsic06", "string", "industry_code_anzsic06", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Redshift Cluster
RedshiftCluster_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="esgaq-src",
    table_name="src_redshift_write_poc",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="RedshiftCluster_node3",
)

job.commit()
