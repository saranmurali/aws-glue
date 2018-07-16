import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "pa_technographic_data", table_name = "palo_alto_technographic", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "pa_technographic_data", table_name = "palo_alto_technographic", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("﻿url", "string", "﻿url", "string"), ("company", "string", "company", "string"), ("country", "string", "country", "string"), ("vendor", "string", "vendor", "string"), ("product", "string", "product", "string"), ("date first verified", "string", "date first verified", "string"), ("date last verified", "string", "date last verified", "string"), ("intensity", "long", "intensity", "long"), ("category parent", "string", "category parent", "string"), ("category", "string", "category", "string"), ("attributes", "string", "attributes", "string"), ("revenue", "string", "revenue", "string"), ("employees", "string", "employees", "string"), ("top level industry", "string", "top level industry", "string"), ("sub level industry", "string", "sub level industry", "string"), ("hq address", "string", "hq address", "string"), ("hq city", "string", "hq city", "string"), ("hq state", "string", "hq state", "string"), ("hq zip", "string", "hq zip", "string"), ("hq country", "string", "hq country", "string"), ("hq phone", "string", "hq phone", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("﻿url", "string", "﻿url", "string"), ("company", "string", "company", "string"), ("country", "string", "country", "string"), ("vendor", "string", "vendor", "string"), ("product", "string", "product", "string"), ("date first verified", "string", "date first verified", "string"), ("date last verified", "string", "date last verified", "string"), ("intensity", "long", "intensity", "long"), ("category parent", "string", "category parent", "string"), ("category", "string", "category", "string"), ("attributes", "string", "attributes", "string"), ("revenue", "string", "revenue", "string"), ("employees", "string", "employees", "string"), ("top level industry", "string", "top level industry", "string"), ("sub level industry", "string", "sub level industry", "string"), ("hq address", "string", "hq address", "string"), ("hq city", "string", "hq city", "string"), ("hq state", "string", "hq state", "string"), ("hq zip", "string", "hq zip", "string"), ("hq country", "string", "hq country", "string"), ("hq phone", "string", "hq phone", "string")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://b2b-dde/Palo_Alto_Technographic/output-files"}, format = "json", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://b2b-dde/Palo_Alto_Technographic/output-files"}, format = "json", transformation_ctx = "datasink2")
job.commit()