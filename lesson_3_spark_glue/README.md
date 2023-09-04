# Lesson 3: Spark & Human Balance

In this project, I use Spark and Glue to build a data lakehouse solution for sensor data. This data will be shared with researchers only if the customers agree, so we process only the data of those who have agreed to use the data for research purposes. This data will eventually be transformed and used for machine learning purposes.

First, the data is ingested as a Glue table in the following SQL tables:
1. accelerometer_landing.sql
2. customer_landing.sql

*  `customer_landing_to_trusted_zone.py`: then, the data is processed so that we only include customers who have consented to the research.
*  `accelerometer_landing_to_trusted_zone.py`: accelerometer data is also processed and filtered for customers who have consented to the research
* `customers_trusted_to_curated_zone.py`: the data is further filtered so that we only include data of those who have accelerometer data
* `step_trainer_landing_to_trusted_zone.py`: the step trainer data is filtered for customers who have consented to the research
* `machine_learning_curated_zone.py`: the data is finally processed to include aggregated information from step trainer and accelerometer.