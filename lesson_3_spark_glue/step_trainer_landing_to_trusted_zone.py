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

# Script generated for node step trainer trusted
steptrainertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="steptrainertrusted_node1",
)

# Script generated for node customer curated
customercurated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customercurated_node1",
)

# Script generated for node filter step trainer
filtersteptrainerjoin_node1 = Join.apply(
    frame1=customercurated_node1,
    frame2=steptrainertrusted_node1,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="filtersteptrainerjoin_node1",
)

# Script generated for node select_steptrainer_fields
select_steptrainer_fields_node1693800914951 = SelectFields.apply(
    frame=filtersteptrainerjoin_node1,
    paths=["sensorReadingTime", "serialNumber", "distanceFromObject"],
    transformation_ctx="select_steptrainer_fields_node1693800914951",
)

# Script generated for node customer_curated
step_trainer_node3 = glueContext.write_dynamic_frame.from_options(
    frame=select_steptrainer_fields_node1693800914951,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_node3",
)

job.commit()
