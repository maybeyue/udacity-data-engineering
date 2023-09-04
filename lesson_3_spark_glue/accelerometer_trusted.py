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

# Script generated for node Accelerometer landing
Accelerometerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometerlanding_node1",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1693586755414 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1693586755414",
)

# Script generated for node Join
Join_node1693586721590 = Join.apply(
    frame1=Accelerometerlanding_node1,
    frame2=CustomerTrustedZone_node1693586755414,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1693586721590",
)

# Script generated for node Drop Fields
DropFields_node1693586885344 = DropFields.apply(
    frame=Join_node1693586721590,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1693586885344",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1693586885344,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
