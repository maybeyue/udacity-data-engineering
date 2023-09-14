# Udacity Data Engineering Course

## Lesson 1 Data Modeling

### Lesson Overview
* Data modeling for relational and NoSQL databases
* An introduction to data modeling for relational databases with PostgreSQL
* An introduction to data modeling for NoSQL databases with Apache Cassandra

### Tools Used
* PostgreSQL
* Apache Cassandra

### Project Overview
* Dataset: Music streaming events data
* Create an Apache Cassandra database to create queries on song play data to answer some analysis questions
* Design tables to answer queries outlined in the project
* Build ETL pipeline

## Lesson 2 Cloud Data Warehouse

### Lesson Overview
* Data warehouse architecture
* Extracting, transforming, and loading data (ETL)
* Cloud data warehouses

### Tools Used
* AWS Redshift
* Amazon S3

### Project Overview
* Dataset: Music streaming events and song data
* Move data from S3 directory of JSON event logs and JSON metadata of the songs into a Redshift cluster
* Build ETL pipeline that extracts data from S3, stages them in Redshift, and transforms the data into dimensional tables

## Lesson 3 Spark and Data Lakes

### Lesson Overview
* Big Data Engineering and Ecosystem
* Apache Spark
* Using Spark with Amazon Web Services (AWS)
* Building a data lake house on AWS

### Tools Used
* Apache Spark
* PySpark
* AWS Glue
* AWS S3

### Project Overview
* Dataset: Human step sensor, user, and mobile app data
* Extract data produced by sensor and mobile app data
* Curate into data lakehouse solution on AWS using S3, Glue, and PySpark


## Lesson 4 Automating Data Pipelines

### Lesson Overview
* DAGs in Apache Airflow
* S3 to Redshift DAG
* Data lineage in Airflow
* Data pipeline schedules

### Tools Used
* AWS Redshift Serverless
* Apache Airflow
* AWS S3

### Project Overview
* Dataset: Music streaming events and song data
* Create custom operators to stage the data, fill the datawarehouse, and validate the final data

