# Data Warehouse Project

## Background
The data provided is meant to mimic a music streaming startup who wants to move their processes onto a cloud. The data resides in S3 in a directory of JSON logs along with a directory of songs in the app.

The goal of this project is to build an ETL pipeline that ingests the data from S3, stages the data onto Redshift, and transfroms the data into a set of dimensional tables in order for the analytics team to determine insights. Here, we want to specifically understand user behavior regarding how people listen to music.

## Database schema design
This database follows a star schema, where the fact table is the log of song plays. The dimensions of the table include user, song, artist, and time.

The following is the database schema:

![schema image](https://github.com/maybeyue/udacity-data-engineering/blob/main/schema.jpg)

This schema provides a simple, performative query execution that's easy to maintain.