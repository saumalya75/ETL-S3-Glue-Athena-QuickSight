import sys
from itertools import chain
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
# from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when, col, lit, create_map, collect_list
from pyspark.sql.types import IntegerType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

COMPOSITE_KEYS = ["Measure_Level", "Measure_Element", "Measure_Unit"]


def source_data_cleanup(in_dfc: DynamicFrame):
    in_df = in_dfc.toDF()
    projected_source_df = in_df.select(
        "Claims_Thru_Dt",
        "Measure_Level",
        "Measure_Element",
        "Measure_Unit",
        "Value"
    )
    return projected_source_df


def unique_record_type_counts(in_df):
    return in_df \
        .drop('Claims_Thru_Dt') \
        .groupBy(*COMPOSITE_KEYS) \
        .count() \
        .withColumnRenamed("Count(Value)", "Total_Count")


def missing_value_validation(in_df):
    derived_columns_df = in_df \
        .withColumn(
            "Overall_Count",
            when(
                (
                    (in_df.Measure_Level.isin('COVID-19 Cases'))
                    & (in_df.Measure_Unit.isin('Beneficiary Count'))
                    & (in_df.Measure_Element.isin('Overall'))
                ),
                in_df.Value.cast(IntegerType())
            )
            .otherwise(0)
        ) \
        .withColumn(
            "Missing_Count",
            when(
                (
                    (in_df.Measure_Level.isin('COVID-19 Cases by State'))
                    & (in_df.Measure_Unit.isin('Beneficiary Count'))
                    & (in_df.Measure_Element.isin('Missing Data'))
                ),
                in_df.Value.cast(IntegerType())
            )
            .otherwise(0)
        ) \
        .withColumn(
            "State_Count",
            when(
                (
                    (in_df.Measure_Level.isin('COVID-19 Cases by State'))
                    & (in_df.Measure_Unit.isin('Beneficiary Count'))
                    & (~ in_df.Measure_Element.isin('Missing Data'))
                ),
                in_df.Value.cast(IntegerType())
            )
            .otherwise(0)
        ) \
        .select('Claims_Thru_Dt', 'Overall_Count', 'State_Count', 'Missing_Count') \
        .groupBy('Claims_Thru_Dt').sum('Overall_Count', 'State_Count', 'Missing_Count') \
        .withColumnRenamed('Sum(Overall_Count)', 'Overall_Count') \
        .withColumnRenamed('Sum(State_Count)', 'Total_State_Count') \
        .withColumnRenamed('Sum(Missing_Count)', 'Missing_Count') \
        .withColumn("Count_Match_Flag", col('Overall_Count') == (col('Total_State_Count') + col('Missing_Count')))

    return derived_columns_df


def total_count_by_state_per_day(in_df):
    dt_value_map_df =  in_df \
        .filter(
            (in_df.Measure_Level.isin('COVID-19 Cases by State'))
            & (in_df.Measure_Unit.isin('Beneficiary Count'))
            & (~in_df.Measure_Element.isin('Missing Data'))
        ) \
        .select("Claims_Thru_Dt", "Measure_Level", "Measure_Element", "Value") \
        .withColumnRenamed("Value", "Count") \
        .withColumn(
            'mapped_count_data',
            create_map(
                list(
                    chain(
                        *((lit(column), column) for column in 'Claims_Thru_Dt,Count'.split(','))
                    )
                )
            )
        ) \
        .select("Measure_Level", "Measure_Element", "mapped_count_data")

    state_wise_day_count_list_df = dt_value_map_df \
        .groupBy("Measure_Level", "Measure_Element") \
        .agg(collect_list("mapped_count_data")) \
        .withColumnRenamed("collect_list(mapped_count_data)", "Day_Wise_Count")

    state_name_list_map_df = state_wise_day_count_list_df \
        .withColumn(
            'State_Name_List_Map',
            create_map(
                [
                    "Measure_Element", "Day_Wise_Count"
                ]
            )
        ) \
        .select("Measure_Level", "State_Name_List_Map")

    state_data_list_df = state_name_list_map_df \
        .groupBy("Measure_Level") \
        .agg(collect_list("State_Name_List_Map")) \
        .withColumnRenamed("collect_list(State_Name_List_Map)", "State_Data_List")

    measure_level = state_data_list_df.select("Measure_Level").first()[0]
    final_data_map_df = state_data_list_df \
        .withColumnRenamed("State_Data_List", measure_level) \
        .drop("Measure_Level")

    return final_data_map_df


medicare_covid_data_dfc = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "escaper": "", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://glue-handson-data/COVID-19-Data-Exploration/SRC/Medicare_COVID_19_Data_Snapshot/"],
        "recurse": True
    },
    transformation_ctx="medicare_covid_data_dfc"
)

cleanedup_df = source_data_cleanup(medicare_covid_data_dfc)

missing_value_validation_df = missing_value_validation(cleanedup_df)
missing_value_validation_dfc = DynamicFrame.fromDF(
    missing_value_validation_df.coalesce(1),
    glueContext,
    "missing_value_validation_dfc"
)

unique_record_type_counts_df = unique_record_type_counts(cleanedup_df)
unique_record_type_counts_dfc = DynamicFrame.fromDF(
    unique_record_type_counts_df.coalesce(1),
    glueContext,
    "unique_record_type_counts_dfc"
)

final_df = total_count_by_state_per_day(cleanedup_df)
final_dfc = DynamicFrame.fromDF(final_df.coalesce(1), glueContext, "final_dfc")


missing_value_validation_sink = glueContext.write_dynamic_frame.from_options(
    frame=missing_value_validation_dfc,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://glue-handson-data/COVID-19-Data-Exploration/TGT/missing_value_validation/",
        "partitionKeys": []
    },
    transformation_ctx="missing_value_validation_sink"
)
unique_record_type_counts_sink = glueContext.write_dynamic_frame.from_options(
    frame=unique_record_type_counts_dfc,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://glue-handson-data/COVID-19-Data-Exploration/TGT/unique_record_type_counts/",
        "partitionKeys": []
    },
    transformation_ctx="unique_record_type_counts_sink"
)
medicare_covid_data_sink = glueContext.write_dynamic_frame.from_options(
    frame=final_dfc,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://glue-handson-data/COVID-19-Data-Exploration/TGT/SummaryReport/",
        "partitionKeys": []
    },
    transformation_ctx="medicare_covid_data_sink"
)
job.commit()