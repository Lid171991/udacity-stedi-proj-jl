import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1754060441880 = glueContext.create_dynamic_frame.from_catalog(database="stedi-database-jl", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1754060441880")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1754060507106 = glueContext.create_dynamic_frame.from_catalog(database="stedi-database-jl", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1754060507106")

# Script generated for node Join
Join_node1754060537663 = Join.apply(frame1=AccelerometerLanding_node1754060507106, frame2=CustomerTrusted_node1754060441880, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1754060537663")

# Script generated for node Select Fields
SelectFields_node1754060596816 = SelectFields.apply(frame=Join_node1754060537663, paths=["user", "timestamp", "x", "y", "z"], transformation_ctx="SelectFields_node1754060596816")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=SelectFields_node1754060596816, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754060416389", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1754060678791 = glueContext.getSink(path="s3://stedi-project-bucket-jl/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1754060678791")
AccelerometerTrusted_node1754060678791.setCatalogInfo(catalogDatabase="stedi-database-jl",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1754060678791.setFormat("json")
AccelerometerTrusted_node1754060678791.writeFrame(SelectFields_node1754060596816)
job.commit()
