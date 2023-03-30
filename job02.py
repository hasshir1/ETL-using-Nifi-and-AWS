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

# Script generated for node Amazon S3
AmazonS3_node1677088761773 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://chicago-processed-bucket/Chicago_crimes/date_district/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1677088761773",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://chicago-processed-bucket/Chicago_crimes/crime_description/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1677088750738 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://chicago-processed-bucket/Chicago_crimes/arrest_by_type/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1677088750738",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1677088763821 = ApplyMapping.apply(
    frame=AmazonS3_node1677088761773,
    mappings=[
        ("crime_date", "string", "crime_date", "string"),
        ("ID", "bigint", "ID", "int"),
        ("District", "double", "District", "int"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1677088763821",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("ID", "bigint", "ID", "int"),
        ("Case_Number", "string", "Case_Number", "string"),
        ("Date", "string", "Date", "string"),
        ("Block", "string", "Block", "string"),
        ("IUCR", "string", "IUCR", "string"),
        ("Primary_Type", "string", "Primary_Type", "string"),
        ("Description", "string", "Description", "string"),
        ("Location_Description", "string", "Location_Description", "string"),
        ("Arrest", "boolean", "Arrest", "boolean"),
        ("Domestic", "boolean", "Domestic", "boolean"),
        ("Beat", "bigint", "Beat", "long"),
        ("District", "double", "District", "int"),
        ("Ward", "double", "Ward", "int"),
        ("Community_Area", "double", "Community_Area", "int"),
        ("FBI_Code", "string", "FBI_Code", "string"),
        ("X_Coordinate", "double", "X_Coordinate", "double"),
        ("Y_Coordinate", "double", "Y_Coordinate", "double"),
        ("Year", "double", "Year", "int"),
        ("Updated_On", "string", "Updated_On", "string"),
        ("Latitude", "double", "Latitude", "string"),
        ("Longitude", "double", "Longitude", "string"),
        ("Location", "string", "Location", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1677088753142 = ApplyMapping.apply(
    frame=AmazonS3_node1677088750738,
    mappings=[
        ("crime_date", "string", "crime_date", "string"),
        ("Primary_Type", "string", "Primary_Type", "string"),
        ("Ward", "double", "Ward", "int"),
        ("Arrest", "bigint", "Arrest", "int"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1677088753142",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1677088765596 = glueContext.write_dynamic_frame.from_catalog(
    frame=ChangeSchemaApplyMapping_node1677088763821,
    database="chicago-redshift",
    table_name="dev_public_chicago_crimes_by_date_district",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node1677088765596",
)

# Script generated for node Redshift Cluster
RedshiftCluster_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="chicago-redshift",
    table_name="dev_public_chicago_crimes_details",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="RedshiftCluster_node3",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1677088755847 = glueContext.write_dynamic_frame.from_catalog(
    frame=ChangeSchemaApplyMapping_node1677088753142,
    database="chicago-redshift",
    table_name="dev_public_chicago_crimes_by_ward_arrest",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node1677088755847",
)

job.commit()
