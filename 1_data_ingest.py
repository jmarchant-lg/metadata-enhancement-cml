# Access data from Cloud Storage or the Hive metastore
#
# Accessing data from [the Hive metastore](https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-accessing-data-from-apache-hive.html)
# that comes with CML only takes a few more steps.
# But first we need to fetch the data from Cloud Storage and save it as a Hive table.
#
# > Specify `STORAGE` as an
# > [environment variable](https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-environment-variables.html)
# > in your project settings containing the Cloud Storage location used by the DataLake to store
# > Hive data. On AWS it will `s3a://[something]`, on Azure it will be `abfs://[something]` and on
# > on prem CDSW cluster, it will be `hdfs://[something]`
#
# This was done for you when you ran `0_bootstrap.py`, so the following code is set up to run as is.
# It begins with imports and creating a `SparkSession`.

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *



spark = SparkSession.builder.appName("PythonSQL").config("spark.yarn.access.hadoopFileSystems",os.getenv('STORAGE')).getOrCreate()


# If you know data already, give schema upfront. This is good practice as Spark will
# read *all* the Data if you try infer the schema.

# Now we can read in the data from Cloud Storage into Spark...

storage = os.environ['STORAGE']

hollow_processed = spark.read.csv(
  "{}/datalake/data/content_metadata/hollow_processed.csv".format(
    storage),
  header=True,
  sep=',',
  nullValue='NA'
  )

# ...and inspect the data.

# hollow_processed.show()

hollow_processed.printSchema()

# Now we can store the Spark DataFrame as a file in the local CML file system
# *and* as a table in Hive used by the other parts of the project.

hollow_processed.coalesce(1).write.csv(
    "file:/home/cdsw/raw/hollow-processed/",
    mode="overwrite",
    header=True
)

spark.sql("show databases").show()

spark.sql("show tables in default").show()

# Create the Hive table
# This is here to create the table in Hive used be the other parts of the project, if it
# does not already exist.

if ('hollow_processed' not in list(spark.sql("show tables in default").toPandas()['tableName'])):
    print("creating the hollow_processed database")
    hollow_processed\
        .write.format("parquet")\
        .mode("overwrite")\
        .saveAsTable(
            'default.hollow_processed'
        )

# Show the data in the hive table
# spark.sql("select * from default.hollow_processed").show()

# To get more detailed information about the hive table you can run this:
spark.sql("describe formatted default.hollow_processed").toPandas()

# Other ways to access data

# To access data from other locations, refer to the
# [CML documentation](https://docs.cloudera.com/machine-learning/cloud/import-data/index.html).

# Scheduled Jobs
#
# One of the features of CML is the ability to schedule code to run at regular intervals,
# similar to cron jobs. This is useful for **data pipelines**, **ETL**, and **regular reporting**
# among other use cases. If new data files are created regularly, e.g. hourly log files, you could
# schedule a Job to run a data loading script with code like the above.

# > Any script [can be scheduled as a Job](https://docs.cloudera.com/machine-learning/cloud/jobs-pipelines/topics/ml-creating-a-job.html).
# > You can create a Job with specified command line arguments or environment variables.
# > Jobs can be triggered by the completion of other jobs, forming a
# > [Pipeline](https://docs.cloudera.com/machine-learning/cloud/jobs-pipelines/topics/ml-creating-a-pipeline.html)
# > You can configure the job to email individuals with an attachment, e.g. a csv report which your
# > script saves at: `/home/cdsw/job1/output.csv`.

# Try running this script `1_data_ingest.py` for use in such a Job.

