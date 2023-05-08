# PySpark_Streaming_project
This project documentation outlines the architecture and design of a Spark-based data pipeline that collects and processes tweets from Twitter API. The pipeline includes several stages such as data source system, data collection system, landing data persistence, landing to raw ETL, raw to processed ETL, and shell script coordinator.


The data source system uses a Python script to call the Twitter API and fetch the latest tweets based on the specified query. The data collection system collects the data from the port and stores it in HDFS partitioned by the columns extracted from the tweet's created_at column. The landing data persistence stores the data persistently in its base format, while the landing to raw ETL extracts the dimensions from the landing data and stores them in a directory called "twitter-raw-data".

The raw to processed ETL creates the final fact table(s) by aggregating data from the dimensions created in the previous step. A SparkSQL application is used for this stage, and the output is stored in a directory called "twitter-processed-data". Finally, the shell script coordinator handles the calling of each stage in the pipeline and ensures that the SparkStream is working correctly.

Overall, this pipeline provides a scalable and efficient way to collect and process Twitter data, and the documentation provides detailed instructions on how to implement each stage.
