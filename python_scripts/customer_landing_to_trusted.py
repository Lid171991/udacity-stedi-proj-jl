import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Customer Landing
CustomerLanding_node1754054782848 = glueContext.create_dynamic_frame.from_catalog(database="stedi-database-jl", table_name="customer_landing", transformation_ctx="CustomerLanding_node1754054782848")

# Script generated for node Share With Research
SqlQuery0 = '''
select * from myDataSource
where sharewithresearchasofdate is not null
'''
ShareWithResearch_node1754054876024 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerLanding_node1754054782848}, transformation_ctx = "ShareWithResearch_node1754054876024")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=ShareWithResearch_node1754054876024, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754054744048", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1754054995879 = glueContext.getSink(path="s3://stedi-project-bucket-jl/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1754054995879")
CustomerTrusted_node1754054995879.setCatalogInfo(catalogDatabase="stedi-database-jl",catalogTableName="customer_trusted")
CustomerTrusted_node1754054995879.setFormat("json")
CustomerTrusted_node1754054995879.writeFrame(ShareWithResearch_node1754054876024)
job.commit()
