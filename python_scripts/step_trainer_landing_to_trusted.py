import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1754294513909 = glueContext.create_dynamic_frame.from_catalog(database="stedi-database-jl", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1754294513909")

# Script generated for node Customer Trusted
CustomerTrusted_node1754294468711 = glueContext.create_dynamic_frame.from_catalog(database="stedi-database-jl", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1754294468711")

# Script generated for node Join
StepTrainerLanding_node1754294513909DF = StepTrainerLanding_node1754294513909.toDF()
CustomerTrusted_node1754294468711DF = CustomerTrusted_node1754294468711.toDF()
Join_node1754294543182 = DynamicFrame.fromDF(StepTrainerLanding_node1754294513909DF.join(CustomerTrusted_node1754294468711DF, (StepTrainerLanding_node1754294513909DF['serialnumber'] == CustomerTrusted_node1754294468711DF['serialnumber']), "leftsemi"), glueContext, "Join_node1754294543182")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=Join_node1754294543182, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754294441757", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1754295085379 = glueContext.getSink(path="s3://stedi-project-bucket-jl/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1754295085379")
StepTrainerTrusted_node1754295085379.setCatalogInfo(catalogDatabase="stedi-database-jl",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1754295085379.setFormat("json")
StepTrainerTrusted_node1754295085379.writeFrame(Join_node1754294543182)
job.commit()
