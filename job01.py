import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.pandas as ps
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, BooleanType, DoubleType


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

catalog_df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://chicago-landing-zone"], "recurse": True},
    transformation_ctx="AmazonS3_node1677232296058",
)
df = catalog_df.toDF()
print("Spark columns")
print(df.columns)
pdf = df.toPandas()
print("Dataframe columns")
print(pdf.columns)
#df.show()
df.write.mode("append").parquet("s3://chicago-processed-zone/Chicago_crimes/crime_description")

pdf['crime_date'] = pdf['Date'].apply(lambda x: x.split(" ")[0])
pdf['crime_time'] = pdf['Date'].apply(lambda x: x.split(" ")[1])

df_by_date_district = pdf.groupby(['crime_date','District']).count().reset_index()[['crime_date','ID','District']]
print(df_by_date_district.head())
sdf_by_date_district = spark.createDataFrame(df_by_date_district)
print("date district")
print(sdf_by_date_district.columns)
sdf_by_date_district.write.mode("append").parquet("s3://chicago-processed-zone/Chicago_crimes/date_district")

df_arrest_by_type = pdf.groupby(['crime_date','Primary_Type','Ward']).count().reset_index()[['crime_date','Primary_Type','Ward','Arrest']]
print(df_arrest_by_type.head())
sdf_arrest_by_type = spark.createDataFrame(df_arrest_by_type)
print("arrest by type")
print(sdf_arrest_by_type.columns)
sdf_arrest_by_type.write.mode("append").parquet("s3://chicago-processed-zone/Chicago_crimes/arrest_by_type")



job.commit()
