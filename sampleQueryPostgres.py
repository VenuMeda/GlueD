import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print( f"Starting the job {args['JOB_NAME']}")
def format_table_name(table_name):
    parts = table_name.split('.')  # Split by dot to separate schema and table
    formatted_name = '.'.join(f'"{part}"' for part in parts)  # Wrap each part in double quotes
    return formatted_name

table_name = 'dbo.States'
table_name = format_table_name(table_name)
print(f'table name {table_name}')
query = f'SELECT count(*) as row_count FROM {table_name}'
print(f'sampleQuery {query}')
# Script generated for node PostgreSQL
PostgreSQL_node1725755023823 = glueContext.create_dynamic_frame.from_options(
    connection_type = "postgresql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "dbo.States",
        "sampleQuery" :query,
        "connectionName": "cobank-ods-rds-jdbc-connection-dev",
    },
    transformation_ctx = "PostgreSQL_node1725755023823"
)
PostgreSQL_node1725755023823.show()

job.commit()
