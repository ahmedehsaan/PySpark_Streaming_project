import os
import subprocess


# Set up Twitter stream listener
twitter_listener_command = ['python3', 'project_twitter_listener.py']
twitter_listener_process = subprocess.Popen(twitter_listener_command)


# Stream data into HDFS
twitter_stream_command = ['/opt/spark-3.1.2-bin-hadoop3.2/bin/spark-submit', 'project_spark_application.py']
subprocess.run(twitter_stream_command, check=True)

# Create dimension tables from Hive Table
hive_dimension_command = ['hive', '-f', 'hive_script.hql']
subprocess.run(hive_dimension_command, check=True)

# Run Spark job
spark_job_command = ['/opt/spark-3.1.2-bin-hadoop3.2/bin/spark-submit', 'SparkSQLApp.py']
subprocess.run(spark_job_command, check=True)
