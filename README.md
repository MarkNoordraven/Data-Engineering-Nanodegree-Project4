# Data Engineering Nanodegree Project 4: Data Pipelines with Airflow
Fourth project in the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027)


![https://confirm.udacity.com/PLQJPKUN](https://github.com/MarkNoordraven/Data-Engineering-Nanodegree-CapstoneProject/blob/master/Data%20Engineering%20certificate.PNG)


## Project: Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Instructions:

1. Instantiate airflow with `/opt/start/airflow.sh`
2. Run create_table_dag
3. Run udac_example_dag
