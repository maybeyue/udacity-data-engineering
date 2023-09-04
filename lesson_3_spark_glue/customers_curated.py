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

# Script generated for node customer_trusted
customer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1693799144205 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometertrusted_node1693799144205",
)

# Script generated for node Join
Join_node1693799199811 = Join.apply(
    frame1=customer_trusted_node1,
    frame2=accelerometertrusted_node1693799144205,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1693799199811",
)

# Script generated for node Drop Fields
DropFields_node1693800262162 = DropFields.apply(
    frame=Join_node1693799199811,
    paths=["z", "timeStamp", "user", "y", "x"],
    transformation_ctx="DropFields_node1693800262162",
)

# Script generated for node customer_curated
customer_curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1693800262162,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_curated_node3",
)

job.commit()
