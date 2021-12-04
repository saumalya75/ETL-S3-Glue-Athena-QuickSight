import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "esgaq-src", table_name = "src_redshift_write_poc", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "esgaq-src", table_name = "src_redshift_write_poc", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("year", "long", "year", "long"), ("industry_aggregation_nzsioc", "string", "industry_aggregation_nzsioc", "string"), ("industry_code_nzsioc", "string", "industry_code_nzsioc", "string"), ("industry_name_nzsioc", "string", "industry_name_nzsioc", "string"), ("units", "string", "units", "string"), ("variable_code", "string", "variable_code", "string"), ("variable_name", "string", "variable_name", "string"), ("variable_category", "string", "variable_category", "string"), ("value", "string", "value", "string"), ("industry_code_anzsic06", "string", "industry_code_anzsic06", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]

applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("year", "long", "year", "long"), ("industry_aggregation_nzsioc", "string", "industry_aggregation_nzsioc", "string"), ("industry_code_nzsioc", "string", "industry_code_nzsioc", "string"), ("industry_name_nzsioc", "string", "industry_name_nzsioc", "string"), ("units", "string", "units", "string"), ("variable_code", "string", "variable_code", "string"), ("variable_name", "string", "variable_name", "string"), ("variable_category", "string", "variable_category", "string"), ("value", "string", "value", "string"), ("industry_code_anzsic06", "string", "industry_code_anzsic06", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [catalog_connection = "redshift-connection", connection_options = {"dbtable": "src_redshift_write_poc", "database": "dev"}, redshift_tmp_dir = TempDir, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]

datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dropnullfields3,
    catalog_connection="redshift-connection",
    connection_options={
        "preactions": "truncate table poc.tgt_redshift_write_poc;",
        "dbtable": "poc.tgt_redshift_write_poc",
        "database": "dev"
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="datasink4"
)
job.commit()