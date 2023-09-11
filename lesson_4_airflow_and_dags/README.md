# Lesson 4: Airflow and DAGs

This project automates data pipelines with Airflow. The data is stored in Amazon S3 and moved into Redshift in order to create fact and dimension tables into the appropriate data models. Airflow is used to automate the process and run the data transformation on an hourly basis. 

The dag is transformed as follows:

[!DAG](https://github.com/maybeyue/udacity-data-engineering/blob/main/lesson_4_airflow_and_dags/photos/dag.png)

First, create two staging tables for songs and song play events. Then, load that data from S3 into Redshift. Once those tables are created, load the main fact table, then create the rest of the dimensions table. Finally, we run basic data quality checks to make sure that the data is loaded appropriately and all primary keys contain non-null values. The expected schema is as follows:

![schema image](https://github.com/maybeyue/udacity-data-engineering/blob/main/lesson_2_data_warehouse/schema.jpg)

