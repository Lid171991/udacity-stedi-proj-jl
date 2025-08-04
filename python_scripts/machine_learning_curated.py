import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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
CustomerTrusted_node1754296203710 = glueContext.create_dynamic_frame.from_catalog(database="stedi-database-jl", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1754296203710")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1754296205757 = glueContext.create_dynamic_frame.from_catalog(database="stedi-database-jl", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1754296205757")

# Script generated for node Join
CustomerTrusted_node1754296203710DF = CustomerTrusted_node1754296203710.toDF()
AccelerometerTrusted_node1754296205757DF = AccelerometerTrusted_node1754296205757.toDF()
Join_node1754296238520 = DynamicFrame.fromDF(CustomerTrusted_node1754296203710DF.join(AccelerometerTrusted_node1754296205757DF, (CustomerTrusted_node1754296203710DF['email'] == AccelerometerTrusted_node1754296205757DF['user']), "left"), glueContext, "Join_node1754296238520")

# Script generated for node Drop Fields
DropFields_node1754297319569 = DropFields.apply(frame=Join_node1754296238520, paths=["user", "timestamp", "x", "y", "z"], transformation_ctx="DropFields_node1754297319569")

# Script generated for node Drop Duplicates
DropDuplicates_node1754297358517 =  DynamicFrame.fromDF(DropFields_node1754297319569.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1754297358517")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1754297358517, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754296181247", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1754297661533 = glueContext.getSink(path="s3://stedi-project-bucket-jl/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1754297661533")
CustomerCurated_node1754297661533.setCatalogInfo(catalogDatabase="stedi-database-jl",catalogTableName="customer_curated")
CustomerCurated_node1754297661533.setFormat("json")
CustomerCurated_node1754297661533.writeFrame(DropDuplicates_node1754297358517)
job.commit()
